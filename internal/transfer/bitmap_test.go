package transfer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitmap_SetHasCount(t *testing.T) {
	b := NewBitmap(10)
	require.False(t, b.Has(3))
	b.Set(3)
	b.Set(3)
	require.True(t, b.Has(3))
	require.Equal(t, uint64(1), b.Count())
	require.False(t, b.Full())
	for i := range uint64(10) {
		b.Set(i)
	}
	require.True(t, b.Full())
	require.Equal(t, uint64(10), b.Count())
}

func TestBitmap_RangesMissing(t *testing.T) {
	b := NewBitmap(50)
	require.Nil(t, b.Ranges())
	require.Equal(t, [][2]uint64{{0, 49}}, b.Missing())

	for _, i := range []uint64{0, 1, 2, 37, 40, 41, 49} {
		b.Set(i)
	}
	require.Equal(t,
		[][2]uint64{{0, 2}, {37, 37}, {40, 41}, {49, 49}}, b.Ranges())
	require.Equal(t,
		[][2]uint64{{3, 36}, {38, 39}, {42, 48}}, b.Missing())
}

func TestBitmap_LoadRoundTrip(t *testing.T) {
	b := NewBitmap(13)
	b.Set(0)
	b.Set(12)
	got, err := LoadBitmap(13, b.Bytes())
	require.NoError(t, err)
	require.Equal(t, b.Ranges(), got.Ranges())

	_, err = LoadBitmap(13, []byte{1})
	require.Error(t, err)
}

func TestBitmap_Zero(t *testing.T) {
	b := NewBitmap(0)
	require.True(t, b.Full())
	require.Nil(t, b.Missing())
}
