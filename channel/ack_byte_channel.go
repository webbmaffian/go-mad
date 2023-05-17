package channel

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

type AckByteChannel[K any] struct {
	data       mmap.MMap
	readCond   sync.Cond
	writeCond  sync.Cond
	mu         sync.Mutex
	isMsg      func(msg []byte, key K) bool
	file       *os.File
	head       *header
	blockWrite bool
	closed     bool
}

func NewAckByteChannel[K any](filepath string, capacity int, itemSize int, blockWrite bool, isMsg func(msg []byte, key K) bool) (ch *AckByteChannel[K], err error) {
	ch = &AckByteChannel[K]{
		head:       newHeader(capacity, itemSize),
		blockWrite: blockWrite,
		isMsg:      isMsg,
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

	return
}

func (ch *AckByteChannel[K]) validateHead(fileSize int64) (err error) {
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

	// Cursor index must be less than capacity
	if head.cursorIdx >= head.capacity {
		return errors.New("invalid capacity")
	}

	// A capacity can never be less than the length
	if head.capacity < head.length {
		return errors.New("invalid capacity")
	}

	// A length can never be less than the unread
	if head.length < head.unread {
		return errors.New("invalid unread")
	}

	if fileSize != head.fileSize() {
		return errors.New("invalid file size")
	}

	return
}

func (ch *AckByteChannel[K]) Write(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for ch.head.length == ch.head.capacity {
		if ch.closed || !ch.blockWrite {
			return false
		}

		// Wait until there is space in the buffer
		ch.writeCond.Wait()
	}

	// Calculate the write offset
	idx := ch.index(ch.head.length)
	ch.head.length++
	ch.head.unread++
	cb(ch.slice(idx))
	ch.readCond.Signal()
	return true
}

func (ch *AckByteChannel[K]) Read() []byte {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Wait until there is data in the buffer
	for ch.head.unread == 0 {
		if ch.closed {
			return nil
		}

		ch.readCond.Wait()
	}

	idx := ch.head.cursorIdx
	ch.head.cursorIdx = ch.wrap(ch.head.cursorIdx + 1)
	ch.head.unread--
	return ch.slice(idx)
}

func (ch *AckByteChannel[K]) AckRead(keys ...K) (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for _, k := range keys {
		if ch.head.length <= ch.head.unread {
			break
		}

		idx := ch.head.cursorIdx

		if !ch.isMsg(ch.slice(idx), k) {
			break
		}

		ch.head.startIdx = ch.index(1)
		ch.head.length--
		count++
	}

	ch.writeCond.Broadcast()

	return
}

func (ch *AckByteChannel[K]) ReadAndAck() []byte {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Wait until there is data in the buffer
	for ch.head.unread == 0 {
		if ch.closed {
			return nil
		}

		ch.readCond.Wait()
	}

	idx := ch.head.cursorIdx
	ch.head.cursorIdx = ch.wrap(ch.head.cursorIdx + 1)
	ch.head.startIdx = ch.index(1)
	ch.head.unread--
	ch.head.length--
	return ch.slice(idx)
}

func (ch *AckByteChannel[K]) AckAllRead() (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	count = ch.indexDiff(ch.head.startIdx, ch.head.cursorIdx)
	ch.head.startIdx = ch.head.cursorIdx

	ch.writeCond.Broadcast()

	return
}

func (ch *AckByteChannel[K]) Flush() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.flush()
}

func (ch *AckByteChannel[K]) flush() error {
	return ch.data.Flush()
}

func (ch *AckByteChannel[K]) DoneWriting() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.closed = true
	ch.readCond.Signal()
}

func (ch *AckByteChannel[K]) Close() (err error) {
	ch.DoneWriting()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.closed = true

	if err = ch.flush(); err != nil {
		return
	}

	return ch.file.Close()
}

func (ch *AckByteChannel[K]) slice(index int64) []byte {
	index *= ch.head.itemSize
	index += ch.head.headSize
	return ch.data[index : index+ch.head.itemSize]
}

func (ch *AckByteChannel[K]) index(index int64) int64 {
	return ch.wrap(ch.head.startIdx + index)
}

func (ch *AckByteChannel[K]) indexDiff(index1, index2 int64) int64 {
	return ch.wrap(index1 - index2)
}

func (ch *AckByteChannel[K]) wrap(index int64) int64 {
	return (index + ch.head.capacity) % ch.head.capacity
}
