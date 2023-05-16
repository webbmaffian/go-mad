package channel

import (
	"unsafe"
)

func newHeader(capacity int, itemSize int) *header {
	h := &header{
		capacity: int64(capacity),
		itemSize: int64(itemSize),
	}
	h.headSize = int64(unsafe.Sizeof(*h))

	return h
}

type header struct {
	headSize  int64
	itemSize  int64
	startIdx  int64
	cursorIdx int64
	unread    int64
	length    int64
	capacity  int64
}

func (h header) fileSize() int64 {
	return h.headSize + h.capacity*h.itemSize
}
