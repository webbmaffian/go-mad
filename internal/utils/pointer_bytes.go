package utils

import (
	"unsafe"
)

func PointerToBytes[T any](val *T, length int) []byte {
	header := sliceHeader{
		Data: unsafe.Pointer(val),
		Len:  length,
		Cap:  length,
	}

	return *(*[]byte)(unsafe.Pointer(&header))
}

func BytesToPointer[T any](b []byte) *T {
	header := *(*sliceHeader)(unsafe.Pointer(&b))
	return (*T)(header.Data)
}
