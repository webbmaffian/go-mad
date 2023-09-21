package channel

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

type ByteChannel struct {
	data          mmap.MMap
	readCond      sync.Cond // Awaited by readers, notified by writers.
	writeCond     sync.Cond // Awaited by writers, notified by readers.
	mu            sync.Mutex
	file          *os.File
	head          *header
	closedWriting bool
}

func NewByteChannel(filepath string, capacity int, itemSize int, allowResize ...bool) (ch *ByteChannel, err error) {
	ch = &ByteChannel{
		head: newHeader(capacity, itemSize),
	}

	ch.readCond.L = &ch.mu
	ch.writeCond.L = &ch.mu

	var created bool
	info, err := os.Stat(filepath)

	if err == nil {
		if ch.file, err = os.OpenFile(filepath, os.O_RDWR, 0); err != nil {
			return
		}

		if err = ch.validateHead(info.Size()); err != nil {
			return
		}
	} else if os.IsNotExist(err) {
		if ch.head.capacity == 0 {
			return nil, errors.New("capacity is mandatory")
		}

		if ch.file, err = os.Create(filepath); err != nil {
			return
		}

		if err = ch.file.Truncate(int64(ch.head.fileSize())); err != nil {
			return
		}

		created = true
	} else {
		return
	}

	if ch.data, err = mmap.Map(ch.file, mmap.RDWR, 0); err != nil {
		return
	}

	if created {

		if s := int(ch.head.headSize); copy(ch.data[:ch.head.headSize], utils.PointerToBytes(ch.head, s)) != s {
			return nil, errors.New("failed to write header")
		}

		if err = ch.Flush(); err != nil {
			return
		}
	}

	ch.head = utils.BytesToPointer[header](ch.data[:ch.head.headSize])

	if ch.needResizing(capacity, itemSize) {
		if allowResize == nil || !allowResize[0] {
			ch.Close()
			return nil, errors.New("capacity and/or item size mismatch")
		}

		if err = ch.resize(filepath, capacity, itemSize); err != nil {
			return
		}

		return NewByteChannel(filepath, capacity, itemSize)
	}

	// We don't support acknowledgements here - reset the counter and update length.
	if ch.head.awaitingAck > 0 {
		if ch.head.awaitingAck > ch.head.length {
			ch.head.length = 0
		} else {
			ch.head.length -= ch.head.awaitingAck
		}

		ch.head.awaitingAck = 0
	}

	// Reset statistics
	ch.head.itemsWritten = 0
	ch.head.itemsRead = 0

	return
}

func (ch *ByteChannel) validateHead(fileSize int64) (err error) {
	if fileSize < int64(ch.head.headSize) {
		return errors.New("file too small")
	}

	if ch.file == nil {
		return errors.New("file is not open")
	}

	if _, err = ch.file.Seek(0, io.SeekStart); err != nil {
		return
	}

	b := make([]byte, ch.head.headSize)

	if _, err = io.ReadFull(ch.file, b); err != nil {
		return
	}

	head := utils.BytesToPointer[header](b)

	if head.itemSize < 1 {
		return errors.New("invalid item size")
	}

	// Start index must be less than capacity
	if head.startIdx >= head.capacity {
		return errors.New("invalid capacity")
	}

	// A capacity can never be less than the length
	if head.capacity < head.length {
		return errors.New("invalid capacity")
	}

	if fileSize != head.fileSize() {
		return errors.New("invalid file size")
	}

	return
}

func (ch *ByteChannel) needResizing(capacity int, itemSize int) bool {
	return ch.head.capacity != int64(capacity) || ch.head.itemSize != int64(itemSize)
}

func (ch *ByteChannel) resize(filepath string, capacity int, itemSize int) (err error) {
	newFilepath := filepath + ".new"
	dst, err := NewByteChannel(newFilepath, capacity, itemSize)

	if err != nil {
		return err
	}

	ch.CopyTo(dst)

	if err = dst.Close(); err != nil {
		return
	}

	if err = ch.Close(); err != nil {
		return
	}

	if err = os.Remove(filepath); err != nil {
		return
	}

	return os.Rename(newFilepath, filepath)
}

func (ch *ByteChannel) CopyTo(dst *ByteChannel) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	dst.mu.Lock()
	defer dst.mu.Unlock()

	var i int64

	for i = 0; i < ch.head.capacity; i++ {
		idx := ch.index(i)

		dst.write(func(b []byte) {
			copy(b, ch.slice(idx))
		})
	}

	if ch.head.length < dst.head.capacity {
		dst.head.length = ch.head.length
	} else {
		dst.head.length = dst.head.capacity
	}
}

func (ch *ByteChannel) WriteOrBlock(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting {
		return false
	}

	for !ch.spaceLeft() {
		if ch.closedWriting {
			return false
		}

		// Wait until there is space in the buffer
		ch.writeCond.Wait()
	}

	ch.write(cb)
	return true
}

func (ch *ByteChannel) WriteOrFail(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting || !ch.spaceLeft() {
		return false
	}

	ch.write(cb)
	return true
}

func (ch *ByteChannel) WriteOrReplace(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting {
		return false
	}

	ch.write(cb)
	return true
}

func (ch *ByteChannel) write(cb func([]byte)) {
	idx := ch.index(ch.head.length)
	cb(ch.slice(idx))

	if ch.spaceLeft() {
		ch.head.length++
	} else {
		ch.head.startIdx = ch.index(1)
	}

	ch.head.itemsWritten++
	ch.readCond.Signal()
}

func (ch *ByteChannel) Wait() (ok bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Wait until there is data in the buffer to read
	for ch.empty() {

		// If writing is closed, there will never be any more to read
		if ch.closedWriting {
			return
		}

		ch.readCond.Wait()
	}

	return true
}

func (ch *ByteChannel) ReadToCallback(cb func([]byte) error, undoOnError bool) (err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// If there is nothing to read, fail
	if ch.empty() {
		return ErrEmpty
	}

	err = cb(ch.read())

	if undoOnError && err != nil {
		ch.undoRead()
		ch.readCond.Broadcast()
	} else {
		ch.writeCond.Broadcast()
	}

	return
}

func (ch *ByteChannel) read() []byte {
	idx := ch.index(0)
	ch.head.length--
	ch.head.itemsRead++

	if ch.head.length > 0 {
		ch.head.startIdx = ch.index(1)
	}

	return ch.slice(idx)
}

func (ch *ByteChannel) undoRead() {
	ch.head.startIdx = ch.index(-1)
	ch.head.length++
	ch.head.itemsRead--
}

func (ch *ByteChannel) Flush() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.flush()
}

func (ch *ByteChannel) flush() error {
	return ch.data.Flush()
}

func (ch *ByteChannel) CloseWriting() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.closedWriting {
		ch.closedWriting = true
		ch.writeCond.Signal()
	}
}

func (ch *ByteChannel) Close() (err error) {
	ch.CloseWriting()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.closedWriting = true

	if err = ch.flush(); err != nil {
		return
	}

	return ch.file.Close()
}

func (ch *ByteChannel) Empty() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.empty()
}

func (ch *ByteChannel) empty() bool {
	return ch.len() <= 0
}

func (ch *ByteChannel) SpaceLeft() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.spaceLeft()
}

func (ch *ByteChannel) spaceLeft() bool {
	return ch.len() < ch.cap()
}

func (ch *ByteChannel) Len() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.len()
}

func (ch *ByteChannel) len() int64 {
	return ch.head.length
}

func (ch *ByteChannel) Cap() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.cap()
}

func (ch *ByteChannel) cap() int64 {
	return ch.head.capacity
}

func (ch *ByteChannel) Reset() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.head.startIdx = 0
	ch.head.length = 0
	ch.writeCond.Broadcast()
}

func (ch *ByteChannel) ItemsWritten() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.head.itemsWritten
}

func (ch *ByteChannel) ItemsRead() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.head.itemsRead
}

func (ch *ByteChannel) slice(index int64) []byte {
	index *= ch.head.itemSize
	index += ch.head.headSize
	return ch.data[index : index+ch.head.itemSize]
}

func (ch *ByteChannel) index(index int64) int64 {
	return ch.wrap(ch.head.startIdx + index)
}

func (ch *ByteChannel) wrap(index int64) int64 {
	return (index + ch.head.capacity) % ch.head.capacity
}
