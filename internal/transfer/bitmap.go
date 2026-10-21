package transfer

import (
	"fmt"
	"math/bits"
)

// Bitmap tracks which chunks of a transfer have been received.
type Bitmap struct {
	n     uint64
	bits  []byte
	count uint64
}

// NewBitmap returns an empty bitmap for n chunks.
func NewBitmap(n uint64) *Bitmap {
	return &Bitmap{n: n, bits: make([]byte, (n+7)/8)}
}

// LoadBitmap restores a bitmap for n chunks from its serialized bytes.
func LoadBitmap(n uint64, raw []byte) (*Bitmap, error) {
	if uint64(len(raw)) != (n+7)/8 {
		return nil, fmt.Errorf(
			"bitmap length %d does not match %d chunks", len(raw), n)
	}
	b := &Bitmap{n: n, bits: make([]byte, len(raw))}
	copy(b.bits, raw)
	for _, x := range b.bits {
		b.count += uint64(bits.OnesCount8(x)) //nolint:gosec // 0..8
	}
	return b, nil
}

// Bytes returns the serialized bitmap.
func (b *Bitmap) Bytes() []byte { return b.bits }

// Len returns the number of chunks the bitmap tracks.
func (b *Bitmap) Len() uint64 { return b.n }

// Set marks chunk i as received.
func (b *Bitmap) Set(i uint64) {
	if !b.Has(i) {
		b.bits[i/8] |= 1 << (i % 8)
		b.count++
	}
}

// Has reports whether chunk i is received.
func (b *Bitmap) Has(i uint64) bool { return b.bits[i/8]&(1<<(i%8)) != 0 }

// Count returns the number of received chunks.
func (b *Bitmap) Count() uint64 { return b.count }

// Full reports whether every chunk is received.
func (b *Bitmap) Full() bool { return b.Count() == b.n }

// Ranges returns received chunks as inclusive [from, to] ranges.
func (b *Bitmap) Ranges() [][2]uint64 { return b.ranges(true) }

// Missing returns not-yet-received chunks as inclusive [from, to] ranges.
func (b *Bitmap) Missing() [][2]uint64 { return b.ranges(false) }

func (b *Bitmap) ranges(want bool) [][2]uint64 {
	var out [][2]uint64
	for i := range b.n {
		if b.Has(i) != want {
			continue
		}
		if len(out) > 0 && out[len(out)-1][1] == i-1 {
			out[len(out)-1][1] = i
		} else {
			out = append(out, [2]uint64{i, i})
		}
	}
	return out
}
