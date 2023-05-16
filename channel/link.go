package channel

import "math"

const ValSize = math.MaxUint16

type Link struct {
	NextIdx  uint32
	Occupied bool
	// 3 bytes left
	Key  uint32
	Size uint32
	Val  [ValSize]byte
}
