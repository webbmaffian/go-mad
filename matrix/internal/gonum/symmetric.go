package gonum

// Symmetric represents a symmetric matrix (where the element at {i, j} equals
// the element at {j, i}). Symmetric matrices are always square.
type Symmetric interface {
	Matrix
	// SymmetricDim returns the number of rows/columns in the matrix.
	SymmetricDim() int
}
