package mmarr

import (
	"unsafe"
)

func newHeader[T any, H any](lenCap ...int) *header[H] {
	var item T

	h := new(header[H])
	h.headSize = int(unsafe.Sizeof(*h))
	h.itemSize = int(unsafe.Sizeof(item))

	if lenCap != nil {
		h.length = lenCap[0]

		if len(lenCap) > 1 {
			h.capacity = lenCap[1]
		} else {
			h.capacity = h.length
		}

		if h.capacity <= 0 {
			h.capacity = 1
		}

		if h.capacity < h.length {
			h.capacity = h.length
		}
	}

	return h
}

type header[H any] struct {
	headSize int
	itemSize int
	length   int
	capacity int
	custom   H
}

func (h header[H]) fileSize() int {
	return h.headSize + h.itemSize*h.capacity
}
