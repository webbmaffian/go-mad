package hashmmap

import (
	"unsafe"

	"github.com/webbmaffian/go-mad/internal/utils"
)

func newHashmmapHeader[K utils.Unsigned, V any]() *hashmmapHeader[K] {
	var key K
	var val V
	var link Link[K, V]

	h := &hashmmapHeader[K]{
		buckets: 255,
	}
	h.headSize = K(unsafe.Sizeof(*h))
	h.keySize = K(unsafe.Sizeof(key))
	h.valSize = K(unsafe.Sizeof(val))
	h.linkSize = K(unsafe.Sizeof(link))

	return h
}

type hashmmapHeader[K utils.Unsigned] struct {
	headSize K
	keySize  K
	valSize  K
	linkSize K
	capacity K
	length   K
	buckets  K
}

func (h hashmmapHeader[K]) fileSize() K {
	return h.headSize + h.buckets*h.keySize + h.capacity*h.linkSize
}
