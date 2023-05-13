package matrix

import (
	"unsafe"

	"github.com/webbmaffian/go-mad/matrix/internal/gonum"
	"github.com/webbmaffian/go-mad/mmarr"
)

var (
	_ gonum.Symmetric = (*SymMatrix[float64])(nil)
	_ gonum.Mutable   = (*SymMatrix[float64])(nil)
)

func NewSym[T any](filepath string, size int) (m *SymMatrix[T], err error) {
	arr, err := mmarr.New[T](filepath, handshakes(size))

	if err != nil {
		return
	}

	m = &SymMatrix[T]{
		arr:  arr,
		size: countFromHandshakes(arr.Len()),
	}

	return
}

func OpenSymRO[T any](filepath string) (m *SymMatrix[T], err error) {
	arr, err := mmarr.OpenRO[T](filepath)

	if err != nil {
		return
	}

	m = &SymMatrix[T]{
		arr:  arr,
		size: countFromHandshakes(arr.Len()),
	}

	return
}

type SymMatrix[T any] struct {
	arr  *mmarr.Array[T, struct{}]
	size int
}

// Dims returns the dimensions (rows + columns) of a Matrix.
func (m *SymMatrix[T]) Dims() (r, c int) {
	return m.size, m.size
}

// SymmetricDim returns the number of rows/columns in the matrix.
func (m *SymMatrix[T]) SymmetricDim() int {
	return m.size
}

// T returns the transpose of the Matrix. Whether T returns a copy of the
// underlying data is implementation dependent.
func (m *SymMatrix[T]) T() gonum.Matrix {
	var v T
	var t any = v

	if _, ok := t.(float64); ok {
		return (*SymMatrix[float64])(unsafe.Pointer(m))
	}

	return nil
}

// At returns the value of a matrix element at row i, column j.
func (m *SymMatrix[T]) At(i, j int) T {
	return *m.arr.Get(m.pos(i, j))
}

// Set alters the matrix element at row i, column j to v.
func (m *SymMatrix[T]) Set(i, j int, val T) {
	m.arr.Set(m.pos(i, j), &val)
}

// At returns the value of a matrix element at row i, column j.
func (m *SymMatrix[T]) Get(i, j int) *T {
	return m.arr.Get(m.pos(i, j))
}

func (m *SymMatrix[T]) Rows() int {
	return m.size
}

func (m *SymMatrix[T]) Cols() int {
	return m.size
}

func (m *SymMatrix[T]) Flush() error {
	return m.arr.Flush()
}

func (m *SymMatrix[T]) Close() error {
	return m.arr.Close()
}

func (m *SymMatrix[T]) pos(i, j int) int {
	i, j = maxMin(i, j)
	return ((j * (m.size - 1)) - ((j * (j + 1)) / 2)) + i - 1
}

func maxMin(a, b int) (max, min int) {
	if a > b {
		return a, b
	} else if a < b {
		return b, a
	}

	panic("A and B cannot be the same value")
}
