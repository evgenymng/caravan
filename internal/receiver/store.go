package receiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Store owns the receiver's on-disk state: one directory per transfer
// under <dir>/.caravan with metadata, a chunk bitmap and the .part file.
type Store struct {
	dir         string
	fsync       bool
	preemptWait time.Duration

	mu     sync.Mutex
	active map[string]*Transfer
}

// Summary is what `status` shows for a stored transfer.
type Summary struct {
	transfer.Meta
	Status         transfer.Status `json:"status"`
	ReceivedChunks uint64          `json:"receivedChunks"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

// Transfer is an exclusively held, in-progress transfer.
type Transfer struct {
	store    *Store
	dir      string
	meta     transfer.Meta
	status   transfer.Status
	bitmap   *transfer.Bitmap
	part     *os.File
	preempt  func()
	done     chan struct{}
	released bool
	// finalPath is set once the file has been moved into place.
	finalPath string
}

type metaOnDisk struct {
	transfer.Meta
	Status    transfer.Status `json:"status"`
	FinalPath string          `json:"finalPath,omitempty"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// NewStore creates dir and its state directory if needed.
func NewStore(dir string, fsync bool) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, stateDirName), 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Store{
		dir:         dir,
		fsync:       fsync,
		preemptWait: 30 * time.Second,
		active:      map[string]*Transfer{},
	}, nil
}

// Scan lists stored transfers without touching them.
func (s *Store) Scan() ([]Summary, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, stateDirName))
	if err != nil {
		return nil, err
	}
	var out []Summary
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(s.dir, stateDirName, e.Name())
		md, err := readMeta(dir)
		if err != nil {
			continue // not ours or damaged; leave it alone
		}
		received := md.Chunks // terminal states keep no bitmap
		if raw, err := os.ReadFile(filepath.Join(dir, bitmapFile)); err == nil {
			if b, err := transfer.LoadBitmap(md.Chunks, raw); err == nil {
				received = b.Count()
			}
		}
		if md.Status == transfer.StatusFailed {
			received = 0
		}
		out = append(out, Summary{
			Meta: md.Meta, Status: md.Status,
			ReceivedChunks: received, UpdatedAt: md.UpdatedAt,
		})
	}
	return out, nil
}

// Recover marks transfers a crashed receiver left in "transferring" as
// interrupted. Only the receiver itself calls this, at startup.
func (s *Store) Recover() error {
	list, err := s.Scan()
	if err != nil {
		return err
	}
	for _, t := range list {
		if t.Status != transfer.StatusTransferring {
			continue
		}
		dir := filepath.Join(s.dir, stateDirName, t.TransferID)
		md, err := readMeta(dir)
		if err != nil {
			continue
		}
		md.Status = transfer.StatusInterrupted
		if err := s.writeMeta(dir, md); err != nil {
			return err
		}
	}
	return nil
}

// Acquire opens the transfer described by meta for exclusive use. An
// existing state directory is resumed (its chunk size wins over meta's);
// a session already holding the same id is preempted via its closer.
func (s *Store) Acquire(meta transfer.Meta, closer func()) (*Transfer, error) {
	if err := meta.Validate(1 << 31); err != nil {
		return nil, err
	}
	s.mu.Lock()
	old := s.active[meta.TransferID]
	s.mu.Unlock()
	if old != nil {
		old.preempt()
		select {
		case <-old.done:
		case <-time.After(s.preemptWait):
			return nil, ErrBusy
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, again := s.active[meta.TransferID]; again {
		return nil, ErrBusy
	}
	t, err := s.open(meta)
	if err != nil {
		return nil, err
	}
	t.preempt = closer
	s.active[meta.TransferID] = t
	return t, nil
}

func (s *Store) open(meta transfer.Meta) (*Transfer, error) {
	dir := filepath.Join(s.dir, stateDirName, meta.TransferID)
	t := &Transfer{store: s, dir: dir, meta: meta, done: make(chan struct{})}

	md, err := readMeta(dir)
	if err == nil && md.Status == transfer.StatusCompleted {
		if _, statErr := os.Stat(md.FinalPath); statErr == nil {
			// The sender never saw COMPLETE: answer as completed again.
			t.meta, t.status, t.finalPath = md.Meta, md.Status, md.FinalPath
			t.bitmap = transfer.NewBitmap(t.meta.Chunks)
			for i := range t.meta.Chunks {
				t.bitmap.Set(i)
			}
			return t, nil
		}
	}
	switch {
	case err == nil && md.Status != transfer.StatusCompleted &&
		md.Status != transfer.StatusFailed:
		if md.FileSize != meta.FileSize || md.Checksum != meta.Checksum {
			return nil, fmt.Errorf("%w: stored size %d/%s, got %d/%s",
				ErrMetaMismatch, md.FileSize, md.Checksum[:8],
				meta.FileSize, meta.Checksum[:8])
		}
		t.meta = md.Meta
		raw, err := os.ReadFile(filepath.Join(dir, bitmapFile))
		if err != nil {
			return nil, fmt.Errorf("read bitmap: %w", err)
		}
		if t.bitmap, err = transfer.LoadBitmap(t.meta.Chunks, raw); err != nil {
			return nil, err
		}
		t.part, err = os.OpenFile(t.partPath(), os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open part file: %w", err)
		}
	case err == nil || errors.Is(err, os.ErrNotExist):
		_ = os.RemoveAll(dir) // terminal leftover or nothing: start fresh
		t.meta.FileName = sanitizeName(meta.FileName, meta.TransferID)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create transfer dir: %w", err)
		}
		t.part, err = os.OpenFile(t.partPath(),
			os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create part file: %w", err)
		}
		size := int64(t.meta.FileSize) //nolint:gosec // validated by Meta
		if err := t.part.Truncate(size); err != nil {
			_ = t.part.Close()
			return nil, fmt.Errorf("allocate part file: %w", err)
		}
		t.bitmap = transfer.NewBitmap(t.meta.Chunks)
		if err := t.saveBitmap(); err != nil {
			_ = t.part.Close()
			return nil, err
		}
	default:
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	if err := t.setStatus(transfer.StatusTransferring); err != nil {
		_ = t.part.Close()
		return nil, err
	}
	return t, nil
}

// Meta returns the stored metadata (chunk size may differ from the
// sender's proposal on resume).
func (t *Transfer) Meta() transfer.Meta { return t.meta }

// Status returns the current lifecycle status.
func (t *Transfer) Status() transfer.Status { return t.status }

// Received returns received chunks as inclusive ranges.
func (t *Transfer) Received() [][2]uint64 { return t.bitmap.Ranges() }

// ReceivedChunks returns the number of received chunks.
func (t *Transfer) ReceivedChunks() uint64 { return t.bitmap.Count() }

// WriteChunk stores verified chunk data. A chunk already present is
// acknowledged without rewriting (dup == true).
func (t *Transfer) WriteChunk(index uint64, data []byte) (bool, error) {
	if index >= t.meta.Chunks {
		return false, fmt.Errorf(
			"%w: %d of %d",
			ErrOutOfRange,
			index,
			t.meta.Chunks,
		)
	}
	if len(
		data,
	) != int(
		t.meta.ChunkLen(index),
	) { //nolint:gosec // len ≤ ChunkSize
		return false, fmt.Errorf("%w: chunk %d has %d bytes, want %d",
			ErrBadChunk, index, len(data), t.meta.ChunkLen(index))
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

// Finalize verifies the whole file, moves it into the downloads
// directory and removes the transfer state. On checksum mismatch the
// transfer is dropped so the next attempt starts from scratch.
func (t *Transfer) Finalize() (string, error) {
	if t.status == transfer.StatusCompleted {
		t.unregister()
		return t.finalPath, nil
	}
	if !t.bitmap.Full() {
		return "", fmt.Errorf(
			"%w: missing %v",
			ErrIncomplete,
			t.bitmap.Missing(),
		)
	}
	if _, err := t.part.Seek(0, 0); err != nil {
		return "", err
	}
	sum, err := transfer.HashReader(context.Background(), t.part)
	if err != nil {
		return "", fmt.Errorf("hash part file: %w", err)
	}
	if sum != t.meta.Checksum {
		t.drop(transfer.StatusFailed)
		return "", fmt.Errorf("%w: got %s, want %s", ErrChecksumMismatch,
			sum[:8], t.meta.Checksum[:8])
	}
	if err := t.part.Close(); err != nil {
		return "", err
	}
	final := uniquePath(filepath.Join(t.store.dir, t.meta.FileName))
	if err := os.Rename(t.partPath(), final); err != nil {
		return "", fmt.Errorf("move finished file: %w", err)
	}
	t.finalPath = final
	t.drop(transfer.StatusCompleted)
	return final, nil
}

// Release records status (normally interrupted), closes the part file
// and lets another session acquire the transfer. Safe to call twice.
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

// drop ends the transfer in a terminal status: the part file and bitmap
// go away, metadata stays as a record (and lets a repeated FINALIZE
// after a lost COMPLETE succeed without resending anything).
func (t *Transfer) drop(status transfer.Status) {
	t.status = status
	_ = t.part.Close()
	_ = os.Remove(t.partPath())
	_ = os.Remove(filepath.Join(t.dir, bitmapFile))
	_ = t.store.writeMeta(t.dir, metaOnDisk{
		Meta: t.meta, Status: status, FinalPath: t.finalPath,
	})
	t.unregister()
}

func (t *Transfer) unregister() {
	t.released = true
	t.store.mu.Lock()
	if t.store.active[t.meta.TransferID] == t {
		delete(t.store.active, t.meta.TransferID)
	}
	t.store.mu.Unlock()
	close(t.done)
}

func (t *Transfer) partPath() string {
	return filepath.Join(t.dir, t.meta.FileName+".part")
}

func (t *Transfer) setStatus(st transfer.Status) error {
	t.status = st
	return t.store.writeMeta(t.dir, metaOnDisk{Meta: t.meta, Status: st})
}

func (t *Transfer) saveBitmap() error {
	return writeAtomic(filepath.Join(t.dir, bitmapFile), t.bitmap.Bytes(),
		t.store.fsync)
}

func (s *Store) writeMeta(dir string, md metaOnDisk) error {
	md.UpdatedAt = time.Now()
	raw, err := json.MarshalIndent(md, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, metaFile), raw, s.fsync)
}

func readMeta(dir string) (metaOnDisk, error) {
	var md metaOnDisk
	raw, err := os.ReadFile(filepath.Join(dir, metaFile))
	if err != nil {
		return md, err
	}
	return md, json.Unmarshal(raw, &md)
}

// writeAtomic writes data via a temp file and rename so a crash never
// leaves a half-written state file behind.
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
	return os.Rename(tmp, path)
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
