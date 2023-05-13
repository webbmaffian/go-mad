package mmarr

import (
	"errors"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

const headSize = 24

// Initialize a new memory-mapped array with a filepath, length and capacity.
// If file doesn't exist, capacity is mandatory. If left out, capacity will
// equal to the length. If capacity and/or length is provided, and the file
// already exists, they must match the values from the file.
// The provided type (`T`) MUST NOT contain any pointer nor slice.
func New[T any](filepath string, lenCap ...int) (arr *Array[T, struct{}], err error) {
	return NewWithHeader[T, struct{}](filepath, lenCap...)
}

func NewWithHeader[T any, H any](filepath string, lenCap ...int) (arr *Array[T, H], err error) {
	arr = &Array[T, H]{
		head: newHeader[T, H](lenCap...),
	}

	if arr.head.itemSize <= 0 {
		return nil, errors.New("item must be at least 1 byte")
	}

	var created bool
	info, err := os.Stat(filepath)

	if err == nil {
		if arr.file, err = os.OpenFile(filepath, os.O_RDWR, 0); err != nil {
			return
		}

		if err = arr.validateHead(info.Size()); err != nil {
			return
		}
	} else if os.IsNotExist(err) {
		if arr.head.capacity == 0 {
			return nil, errors.New("capacity is mandatory")
		}

		if arr.file, err = os.Create(filepath); err != nil {
			return
		}

		if err = arr.file.Truncate(int64(arr.head.fileSize())); err != nil {
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
		if copy(arr.data[:arr.head.headSize], utils.PointerToBytes(arr.head, arr.head.headSize)) != arr.head.headSize {
			return nil, errors.New("failed to write header")
		}

		if err = arr.Flush(); err != nil {
			return
		}
	}

	return
}

func OpenRO[T any](filepath string) (arr *Array[T, struct{}], err error) {
	return OpenROWithHeader[T, struct{}](filepath)
}

func OpenROWithHeader[T any, H any](filepath string) (arr *Array[T, H], err error) {
	arr = &Array[T, H]{
		head: newHeader[T, H](),
	}

	if arr.head.itemSize <= 0 {
		return nil, errors.New("item must be at least 1 byte")
	}

	info, err := os.Stat(filepath)

	if err != nil {
		return
	}

	if arr.file, err = os.OpenFile(filepath, os.O_RDONLY, 0); err != nil {
		return
	}

	if err = arr.validateHead(info.Size()); err != nil {
		return
	}

	if arr.data, err = mmap.Map(arr.file, mmap.RDONLY, 0); err != nil {
		return
	}

	return
}

// Memory-mapped array
type Array[T any, H any] struct {
	data mmap.MMap
	file *os.File
	head *header[H]
}

func (m *Array[T, H]) validateHead(fileSize int64) (err error) {
	if fileSize < int64(m.head.headSize) {
		return errors.New("file too small")
	}

	if m.file == nil {
		return errors.New("file is not open")
	}

	if _, err = m.file.Seek(0, io.SeekStart); err != nil {
		return
	}

	b := make([]byte, m.head.headSize)

	if _, err = io.ReadFull(m.file, b); err != nil {
		return
	}

	head := utils.BytesToPointer[header[H]](b)

	if head.itemSize != m.head.itemSize {
		return errors.New("invalid item size")
	}

	// A capacity can never me less than the length
	if head.capacity < head.length {
		return errors.New("invalid capacity")
	}

	if fileSize != int64(head.fileSize()) {
		return errors.New("invalid file size")
	}

	return
}

func (arr *Array[T, H]) Flush() error {
	return arr.data.Flush()
}

func (arr *Array[T, H]) Close() (err error) {
	if err = arr.Flush(); err != nil {
		return
	}

	return arr.file.Close()
}

func (arr *Array[T, H]) Append(val *T) (pos int) {
	if arr.head.length >= arr.head.capacity {
		return -1
	}

	pos = arr.head.length
	arr.Set(pos, val)
	arr.head.length++
	return
}

func (arr *Array[T, H]) Set(pos int, val *T) {
	idx := arr.posToIdx(pos)
	copy(arr.data[idx:idx+arr.head.itemSize], utils.PointerToBytes(val, arr.head.itemSize))
}

func (arr *Array[T, H]) Get(pos int) *T {
	idx := arr.posToIdx(pos)
	return utils.BytesToPointer[T](arr.data[idx : idx+arr.head.itemSize])
}

func (arr *Array[T, H]) Cap() int {
	return arr.head.capacity
}

func (arr *Array[T, H]) Len() int {
	return arr.head.length
}

func (arr *Array[T, H]) ItemSize() int {
	return arr.head.itemSize
}

func (arr *Array[T, H]) Items() []T {
	return *utils.BytesToPointer[[]T](arr.data[headSize : headSize+arr.head.itemSize*arr.head.length])
}

func (arr Array[T, H]) Head() *H {
	return &arr.head.custom
}

func (arr *Array[T, H]) posToIdx(pos int) int {
	return headSize + (((pos + arr.head.length) % arr.head.length) * arr.head.itemSize)
}
