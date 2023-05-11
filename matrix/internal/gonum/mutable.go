package gonum

// Mutable is a matrix interface type that allows elements to be altered.
type Mutable interface {
	// Set alters the matrix element at row i, column j to v.
	Set(i, j int, v float64)

	Matrix
}
