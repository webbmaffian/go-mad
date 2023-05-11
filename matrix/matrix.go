package matrix

import (
	"unsafe"

	"github.com/webbmaffian/go-mad/matrix/internal/gonum"
	"github.com/webbmaffian/go-mad/mmarr"
)

var _ gonum.Mutable = (*Matrix[float64])(nil)

func New[T any](filepath string, rows int, cols int) (m *Matrix[T], err error) {
	arr, err := mmarr.New[T](filepath, rows*cols)

	if err != nil {
		return
	}

	m = &Matrix[T]{
		arr:  arr,
		rows: rows,
		cols: cols,
	}

	return
}

func OpenRO[T any](filepath string, rows int, cols int) (m *Matrix[T], err error) {
	arr, err := mmarr.OpenRO[T](filepath)

	if err != nil {
		return
	}

	m = &Matrix[T]{
		arr:  arr,
		rows: rows,
		cols: cols,
	}

	return
}

type Matrix[T any] struct {
	arr  *mmarr.Array[T]
	rows int
	cols int
}

// Dims returns the dimensions (rows + columns) of a Matrix.
func (m *Matrix[T]) Dims() (r, c int) {
	return m.rows, m.cols
}

// T returns the transpose of the Matrix. Whether T returns a copy of the
// underlying data is implementation dependent.
func (m *Matrix[T]) T() gonum.Matrix {
	var v T
	var t any = v

	if _, ok := t.(float64); ok {
		return (*Matrix[float64])(unsafe.Pointer(m))
	}

	return nil
}

// Set alters the matrix element at row i, column j to v.
func (m *Matrix[T]) Set(x, y int, v T) {
	m.arr.Set(m.pos(x, y), &v)
}

func (m *Matrix[T]) Get(x, y int) *T {
	return m.arr.Get(m.pos(x, y))
}

func (m *Matrix[T]) At(x, y int) T {
	return *m.arr.Get(m.pos(x, y))
}

func (m *Matrix[T]) Rows() int {
	return m.rows
}

func (m *Matrix[T]) Cols() int {
	return m.cols
}

func (m *Matrix[T]) Flush() error {
	return m.arr.Flush()
}

func (m *Matrix[T]) Close() error {
	return m.arr.Close()
}

func (m *Matrix[T]) pos(i, j int) int {
	return i*m.cols + j
}
