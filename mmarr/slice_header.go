package mmarr

import "unsafe"

type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}
