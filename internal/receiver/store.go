package receiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/evgenymng/caravan/internal/transfer"
)

const (
	stateDirName = ".caravan"
	metaFile     = "metadata.json"
	bitmapFile   = "received.bitmap"
)

// Store owns the receiver's on-disk state: <dir>/.caravan/<id>/ with metadata, bitmap and .part file.
type Store struct {
	dir          string
	fsync        bool
	maxChunkSize uint32

	mu     sync.Mutex
	active map[string]*Transfer
}

// Summary is what `status` shows for a stored transfer.
type Summary struct {
	transfer.Meta
	Status         transfer.Status
	ReceivedChunks uint64
	UpdatedAt      time.Time
}

// Transfer is an exclusively held, in-progress transfer.
type Transfer struct {
	store     *Store
	dir       string
	meta      transfer.Meta
	status    transfer.Status
	bitmap    *transfer.Bitmap
	part      *os.File
	released  bool
	finalPath string
}

type metaOnDisk struct {
	transfer.Meta
	Status    transfer.Status `json:"status"`
	FinalPath string          `json:"finalPath,omitempty"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// NewStore creates dir and its state directory if needed; maxChunkSize limits new transfers only.
func NewStore(dir string, fsync bool, maxChunkSize uint32) (*Store, error) {
	dir, err := filepath.Abs(dir) // finalPath must not depend on cwd
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, stateDirName), 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Store{
		dir:          dir,
		fsync:        fsync,
		maxChunkSize: maxChunkSize,
		active:       map[string]*Transfer{},
	}, nil
}

// Scan lists transfers stored under dir without touching them.
func Scan(dir string) ([]Summary, error) {
	return scan(dir, nil)
}

// Recover marks transfers a crashed receiver left in "transferring" as interrupted and lists all stored ones.
func (s *Store) Recover() ([]Summary, error) {
	return scan(s.dir, s)
}

// Acquire opens or resumes the transfer for exclusive use; stored chunk size wins over meta's.
func (s *Store) Acquire(meta transfer.Meta) (*Transfer, error) {
	if err := meta.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.active[meta.TransferID]; busy {
		return nil, ErrBusy
	}
	t, err := s.open(meta)
	if err != nil {
		return nil, err
	}
	s.active[meta.TransferID] = t
	return t, nil
}

func (s *Store) open(meta transfer.Meta) (*Transfer, error) {
	dir := filepath.Join(s.dir, stateDirName, meta.TransferID)
	t := &Transfer{store: s, dir: dir, meta: meta}

	md, err := readMeta(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("stored metadata unreadable, starting over", "transfer_id", meta.TransferID, "error", err)
	}
	stored := err == nil
	finished := stored && md.Status == transfer.StatusCompleted && sizeIs(md.FinalPath, md.FileSize)
	if finished {
		t.adoptCompleted(md) // sender never saw COMPLETE; Finalize re-hashes the file
		return t, nil
	}
	if err := t.load(md, stored); err != nil {
		return nil, err
	}
	if err := t.setStatus(transfer.StatusTransferring); err != nil {
		_ = t.part.Close()
		return nil, err
	}
	return t, nil
}

func (s *Store) writeMeta(dir string, md metaOnDisk) error {
	md.UpdatedAt = time.Now()
	raw, err := json.MarshalIndent(md, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, metaFile), raw, s.fsync)
}

// Meta returns the stored metadata, whose chunk size may differ from the sender's.
func (t *Transfer) Meta() transfer.Meta { return t.meta }

func (t *Transfer) Status() transfer.Status { return t.status }

// Received returns received chunks as inclusive ranges.
func (t *Transfer) Received() [][2]uint64 { return t.bitmap.Ranges() }

func (t *Transfer) ReceivedChunks() uint64 { return t.bitmap.Count() }

// WriteChunk stores verified chunk data; a chunk already present is not rewritten (dup == true).
func (t *Transfer) WriteChunk(index uint64, data []byte) (bool, error) {
	if index >= t.meta.Chunks {
		return false, fmt.Errorf("%w: %d of %d", ErrOutOfRange, index, t.meta.Chunks)
	}
	if want := t.meta.ChunkLen(index); len(data) != int(want) {
		return false, fmt.Errorf("%w: chunk %d has %d bytes, want %d", ErrBadChunk, index, len(data), want)
	}
	if t.bitmap.Has(index) {
		return true, nil
	}
	off := int64(index) * int64(t.meta.ChunkSize) //nolint:gosec // validated
	if _, err := t.part.WriteAt(data, off); err != nil {
		return false, fmt.Errorf("write chunk %d: %w", index, err)
	}
	if t.store.fsync {
		if err := t.part.Sync(); err != nil {
			return false, fmt.Errorf("sync part file: %w", err)
		}
	}
	t.bitmap.Set(index)
	return false, t.saveBitmap()
}

// Finalize verifies the whole file and moves it into the downloads directory.
func (t *Transfer) Finalize(ctx context.Context) (string, error) {
	if t.status == transfer.StatusCompleted {
		if err := t.verifyFinal(ctx); err != nil {
			return "", err
		}
		t.unregister()
		return t.finalPath, nil
	}
	if !t.bitmap.Full() {
		return "", fmt.Errorf("%w: missing %v", ErrIncomplete, t.bitmap.Missing())
	}
	if _, err := t.part.Seek(0, 0); err != nil {
		return "", err
	}
	if err := t.verify(ctx, t.part); err != nil {
		return "", err
	}
	if err := t.part.Close(); err != nil {
		return "", err
	}
	// Under the store lock so two transfers with the same name cannot
	// pick the same free path. Completion is recorded before the move so
	// a crash in between resumes with a full bitmap instead of losing
	// the .part file.
	t.store.mu.Lock()
	t.finalPath = uniquePath(filepath.Join(t.store.dir, t.meta.FileName))
	err := t.setStatus(transfer.StatusCompleted)
	if err == nil {
		err = os.Rename(t.partPath(), t.finalPath)
	}
	if err == nil && t.store.fsync {
		err = syncDir(t.store.dir)
	}
	t.store.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("move finished file: %w", err)
	}
	t.drop(transfer.StatusCompleted)
	return t.finalPath, nil
}

// Release records status, closes the part file and lets another session acquire the transfer.
func (t *Transfer) Release(status transfer.Status) {
	if t.released {
		return
	}
	if t.status == transfer.StatusCompleted {
		t.unregister()
		return
	}
	_ = t.setStatus(status)
	_ = t.part.Close()
	t.unregister()
}

// adoptCompleted answers a repeated META for a finished transfer whose file is still in place.
func (t *Transfer) adoptCompleted(md metaOnDisk) {
	t.meta, t.status, t.finalPath = md.Meta, md.Status, md.FinalPath
	t.bitmap = transfer.NewBitmap(t.meta.Chunks)
	for i := range t.meta.Chunks {
		t.bitmap.Set(i)
	}
}

// verifyFinal re-hashes the finished file adopted in Acquire, where hashing would hold the store lock.
func (t *Transfer) verifyFinal(ctx context.Context) error {
	f, err := os.Open(t.finalPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	err = t.verify(ctx, f)
	if errors.Is(err, ErrChecksumMismatch) {
		// not fatal: the sender reconnects and sends the file again
		return fmt.Errorf("%w: received file changed", ErrIncomplete)
	}
	return err
}

// verify hashes r against the checksum; a mismatch fails the transfer so the next send starts over.
func (t *Transfer) verify(ctx context.Context, r io.Reader) error {
	sum, err := transfer.HashReader(ctx, r)
	if err != nil {
		return fmt.Errorf("hash file: %w", err)
	}
	if sum == t.meta.Checksum {
		return nil
	}
	t.drop(transfer.StatusFailed)
	return fmt.Errorf("%w: got %s, want %s", ErrChecksumMismatch, sum[:8], t.meta.Checksum[:8])
}

// load resumes stored state; nothing stored, a failed or a damaged one starts from scratch.
func (t *Transfer) load(md metaOnDisk, stored bool) error {
	if !stored || md.Status == transfer.StatusFailed {
		return t.create()
	}
	err := t.resume(md)
	if err == nil {
		return nil
	}
	// a completed transfer whose result was deleted keeps no bitmap: nothing to warn about
	if md.Status != transfer.StatusCompleted {
		slog.Warn("stored state unusable, starting over", "transfer_id", t.meta.TransferID, "error", err)
	}
	return t.create()
}

// resume reopens an interrupted transfer from its stored state, touching t only on success.
func (t *Transfer) resume(md metaOnDisk) error {
	raw, err := os.ReadFile(filepath.Join(t.dir, bitmapFile))
	if err != nil {
		return err
	}
	bitmap, err := transfer.LoadBitmap(md.Chunks, raw)
	if err != nil {
		return err
	}
	part, err := os.OpenFile(filepath.Join(t.dir, md.FileName+".part"), os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	t.meta, t.bitmap, t.part = md.Meta, bitmap, part
	return nil
}

// create starts a transfer from scratch, discarding any leftover state.
func (t *Transfer) create() error {
	if limit := t.store.maxChunkSize; t.meta.ChunkSize > limit {
		return fmt.Errorf("%w: chunk size %d over %d", transfer.ErrInvalidMeta, t.meta.ChunkSize, limit)
	}
	_ = os.RemoveAll(t.dir)
	t.meta.FileName = sanitizeName(t.meta.FileName, t.meta.TransferID)
	if err := os.MkdirAll(t.dir, 0o750); err != nil {
		return fmt.Errorf("create transfer dir: %w", err)
	}
	part, err := os.OpenFile(t.partPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create part file: %w", err)
	}
	t.part = part
	size := int64(t.meta.FileSize) //nolint:gosec // validated by Meta
	if err := t.part.Truncate(size); err != nil {
		_ = t.part.Close()
		return fmt.Errorf("allocate part file: %w", err)
	}
	t.bitmap = transfer.NewBitmap(t.meta.Chunks)
	if err := t.saveBitmap(); err != nil {
		_ = t.part.Close()
		return err
	}
	return nil
}

// drop ends the transfer in a terminal status; only metadata stays, as a record.
func (t *Transfer) drop(status transfer.Status) {
	_ = t.part.Close()
	_ = os.Remove(t.partPath())
	_ = os.Remove(filepath.Join(t.dir, bitmapFile))
	_ = t.setStatus(status)
	t.unregister()
}

func (t *Transfer) unregister() {
	t.released = true
	t.store.mu.Lock()
	if t.store.active[t.meta.TransferID] == t {
		delete(t.store.active, t.meta.TransferID)
	}
	t.store.mu.Unlock()
}

func (t *Transfer) partPath() string {
	return filepath.Join(t.dir, t.meta.FileName+".part")
}

func (t *Transfer) setStatus(st transfer.Status) error {
	t.status = st
	return t.store.writeMeta(t.dir, metaOnDisk{Meta: t.meta, Status: st, FinalPath: t.finalPath})
}

func (t *Transfer) saveBitmap() error {
	return writeAtomic(filepath.Join(t.dir, bitmapFile), t.bitmap.Bytes(), t.store.fsync)
}

// scan reads every stored transfer; with s set, it first marks crashed ones interrupted.
func scan(dir string, s *Store) ([]Summary, error) {
	entries, err := os.ReadDir(filepath.Join(dir, stateDirName))
	if err != nil {
		return nil, err
	}
	var out []Summary
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		tdir := filepath.Join(dir, stateDirName, e.Name())
		md, err := readMeta(tdir)
		if err != nil {
			continue // not ours or damaged; leave it alone
		}
		if s != nil && md.Status == transfer.StatusTransferring {
			md.Status = transfer.StatusInterrupted
			if err := s.writeMeta(tdir, md); err != nil {
				return nil, err
			}
		}
		out = append(out, Summary{
			Meta:           md.Meta,
			Status:         md.Status,
			ReceivedChunks: receivedChunks(tdir, md),
			UpdatedAt:      md.UpdatedAt,
		})
	}
	return out, nil
}

func readMeta(dir string) (metaOnDisk, error) {
	var md metaOnDisk
	raw, err := os.ReadFile(filepath.Join(dir, metaFile))
	if err != nil {
		return md, err
	}
	return md, json.Unmarshal(raw, &md)
}

// writeAtomic writes via a temp file and rename so a crash never leaves a half-written file.
func writeAtomic(path string, data []byte, fsync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil && fsync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if !fsync {
		return nil
	}
	return syncDir(filepath.Dir(path))
}

// syncDir flushes a directory so a rename inside it survives a power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return d.Close()
}

// uniquePath appends " (n)" before the extension until the path is free.
func uniquePath(path string) string {
	if _, err := os.Stat(path); err != nil {
		return path
	}
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	for n := 1; ; n++ {
		p := fmt.Sprintf("%s (%d)%s", base, n, ext)
		if _, err := os.Stat(p); err != nil {
			return p
		}
	}
}

// receivedChunks counts bitmap bits; a completed transfer keeps no bitmap but has every chunk.
func receivedChunks(dir string, md metaOnDisk) uint64 {
	if md.Status == transfer.StatusCompleted {
		return md.Chunks
	}
	raw, err := os.ReadFile(filepath.Join(dir, bitmapFile))
	if err != nil {
		return 0
	}
	b, err := transfer.LoadBitmap(md.Chunks, raw)
	if err != nil {
		return 0
	}
	return b.Count()
}

// sizeIs reports whether the file at path exists with n bytes.
func sizeIs(path string, n uint64) bool {
	st, err := os.Stat(path)
	return err == nil && st.Size() == int64(n) //nolint:gosec // validated by Meta
}
