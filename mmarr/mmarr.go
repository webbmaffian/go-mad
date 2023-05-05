package mmarr

import (
	"errors"
	"io"
	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

const headSize = 24

// Initialize a new memory-mapped array with a filepath, length and capacity.
// If file doesn't exist, capacity is mandatory. If left out, capacity will
// equal to the length. If capacity and/or length is provided, and the file
// already exists, they must match the values from the file.
// The provided type (`T`) MUST NOT contain any pointer nor slice.
func New[T any](filepath string, lenCap ...int) (arr *Array[T], err error) {
	var val T

	arr = &Array[T]{
		itemSize: int(unsafe.Sizeof(val)),
	}

	if arr.itemSize <= 0 {
		return nil, errors.New("invalid item size")
	}

	if lenCap != nil {
		arr.length = lenCap[0]

		if len(lenCap) > 1 {
			arr.capacity = lenCap[1]
		} else {
			arr.capacity = arr.length
		}

		if arr.capacity <= 0 {
			return nil, errors.New("capacity must be at least 1")
		}

		if arr.capacity < arr.length {
			return nil, errors.New("capacity must be greater or equal to the length")
		}
	}

	var created bool
	info, err := os.Stat(filepath)

	if err == nil {
		if arr.file, err = os.OpenFile(filepath, os.O_RDWR, 0); err != nil {
			return
		}

		var head [headSize]byte

		if _, err = io.ReadFull(arr.file, head[:]); err != nil {
			return
		}

		itemSize, length, capacity := int(utils.Endian.Uint64(head[:8])), int(utils.Endian.Uint64(head[8:16])), int(utils.Endian.Uint64(head[16:24]))

		if itemSize != arr.itemSize {
			return nil, errors.New("invalid item size")
		}

		if arr.length != 0 && length != arr.length {
			return nil, errors.New("invalid length")
		}

		if arr.capacity != 0 && capacity != arr.capacity {
			return nil, errors.New("invalid capacity")
		}

		arr.length = length
		arr.capacity = capacity

		if info.Size() != int64(arr.fileSize()) {
			return nil, errors.New("invalid file size")
		}
	} else if os.IsNotExist(err) {
		if arr.capacity == 0 {
			return nil, errors.New("capacity is mandatory")
		}

		if arr.file, err = os.Create(filepath); err != nil {
			return
		}

		if err = arr.file.Truncate(int64(arr.fileSize())); err != nil {
			return
		}

		created = true
	} else {
		return
	}

	if arr.data, err = mmap.Map(arr.file, mmap.RDWR, 0); err != nil {
		return
	}

	if created {
		utils.Endian.PutUint32(arr.data[:8], uint32(arr.itemSize))
		utils.Endian.PutUint32(arr.data[8:16], uint32(arr.length))
		utils.Endian.PutUint32(arr.data[16:24], uint32(arr.capacity))

		if err = arr.Flush(); err != nil {
			return
		}
	}

	return
}

// Memory-mapped array
type Array[T any] struct {
	data     mmap.MMap
	file     *os.File
	itemSize int
	length   int
	capacity int
}

func (arr *Array[T]) Flush() error {
	return arr.data.Flush()
}

func (arr *Array[T]) Close() (err error) {
	if err = arr.Flush(); err != nil {
		return
	}

	return arr.file.Close()
}

func (arr *Array[T]) Append(val *T) (pos int) {
	if arr.length >= arr.capacity {
		return -1
	}

	pos = arr.length
	arr.Set(pos, val)
	arr.length++
	utils.Endian.PutUint64(arr.data[8:16], uint64(arr.length))
	return
}

func (arr *Array[T]) Set(pos int, val *T) {
	idx := arr.posToIdx(pos)
	copy(arr.data[idx:idx+arr.itemSize], arr.valToBytes(val))
}

func (arr *Array[T]) Get(pos int) *T {
	idx := arr.posToIdx(pos)
	return arr.bytesToVal(arr.data[idx : idx+arr.itemSize])
}

func (arr *Array[T]) Cap() int {
	return arr.capacity
}

func (arr *Array[T]) Len() int {
	return arr.length
}

func (arr *Array[T]) ItemSize() int {
	return arr.itemSize
}

func (arr *Array[T]) Items() []T {
	b := arr.data[headSize : headSize+arr.itemSize*arr.length]
	bytesHeader := (*sliceHeader)(unsafe.Pointer(&b))
	header := sliceHeader{
		Data: bytesHeader.Data,
		Len:  arr.length,
		Cap:  arr.capacity,
	}

	return *(*[]T)(unsafe.Pointer(&header))
}

func (arr *Array[T]) fileSize() int {
	return arr.itemSize*arr.capacity + headSize
}

func (arr *Array[T]) posToIdx(pos int) int {
	return headSize + (((pos + arr.length) % arr.length) * arr.itemSize)
}

func (arr Array[T]) valToBytes(val *T) []byte {
	header := sliceHeader{
		Data: unsafe.Pointer(val),
		Len:  arr.itemSize,
		Cap:  arr.itemSize,
	}

	return *(*[]byte)(unsafe.Pointer(&header))
}

func (Array[T]) bytesToVal(b []byte) *T {
	header := *(*sliceHeader)(unsafe.Pointer(&b))
	return (*T)(header.Data)
}
