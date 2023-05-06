package matrix

import (
	"github.com/webbmaffian/go-mad/mmarr"
)

func NewSym[T any](filepath string, size int) (m *Matrix[T], err error) {
	arr, err := mmarr.New[T](filepath, handshakes(size))

	if err != nil {
		return
	}

	m = &Matrix[T]{
		arr:  arr,
		size: countFromHandshakes(arr.Len()),
	}

	return
}

func OpenSymRO[T any](filepath string) (m *Matrix[T], err error) {
	arr, err := mmarr.OpenRO[T](filepath)

	if err != nil {
		return
	}

	m = &Matrix[T]{
		arr:  arr,
		size: countFromHandshakes(arr.Len()),
	}

	return
}

type Matrix[T any] struct {
	arr  *mmarr.Array[T]
	size int
}

func (m *Matrix[T]) Set(x, y int, val T) {
	m.arr.Set(m.pos(x, y), &val)
}

func (m *Matrix[T]) Get(x, y int) *T {
	return m.arr.Get(m.pos(x, y))
}

func (m *Matrix[T]) Len() int {
	return m.size
}

func (m *Matrix[T]) Cap() int {
	return m.size
}

func (m *Matrix[T]) Flush() error {
	return m.arr.Flush()
}

func (m *Matrix[T]) Close() error {
	return m.arr.Close()
}

func (m *Matrix[T]) pos(x, y int) int {
	x, y = maxMin(x, y)
	return ((y * (m.size - 1)) - ((y * (y + 1)) / 2)) + x - 1
}

func maxMin(a, b int) (max, min int) {
	if a > b {
		return a, b
	} else if a < b {
		return b, a
	}

	panic("A and B cannot be the same value")
}
