package channel

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

type AckByteChannel struct {
	data          mmap.MMap
	readCond      sync.Cond
	writeCond     sync.Cond
	mu            sync.Mutex
	file          *os.File
	head          *header
	closedWriting bool
}

func NewAckByteChannel(filepath string, capacity int, itemSize int) (ch *AckByteChannel, err error) {
	ch = &AckByteChannel{
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

	return
}

func (ch *AckByteChannel) validateHead(fileSize int64) (err error) {
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

	if head.itemSize < 1 || head.itemSize != ch.head.itemSize {
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

	if fileSize != head.fileSize() {
		return errors.New("invalid file size")
	}

	return
}

func (ch *AckByteChannel) WriteOrBlock(cb func([]byte)) bool {
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

func (ch *AckByteChannel) WriteOrFail(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting || !ch.spaceLeft() {
		return false
	}

	ch.write(cb)
	return true
}

func (ch *AckByteChannel) WriteOrReplace(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting {
		return false
	}

	idx := ch.index(ch.head.length)
	ch.head.startIdx = ch.index(1)
	cb(ch.slice(idx))
	ch.readCond.Signal()
	return true
}

func (ch *AckByteChannel) write(cb func([]byte)) {
	idx := ch.index(ch.head.length)
	ch.head.length++
	cb(ch.slice(idx))
	ch.readCond.Signal()
}

func (ch *AckByteChannel) ReadOrBlock() (b []byte, ok bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Wait until there is data in the buffer to read
	for !ch.toRead() {

		// If writing is closed, there will never be any more to read
		if ch.closedWriting {
			return
		}

		ch.readCond.Wait()
	}

	return ch.read(), true
}

func (ch *AckByteChannel) ReadOrFail() (b []byte, ok bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// If there is nothing to read, fail
	if !ch.toRead() {
		return
	}

	return ch.read(), true
}

func (ch *AckByteChannel) read() []byte {
	idx := ch.head.cursorIdx
	ch.head.cursorIdx = ch.wrap(ch.head.cursorIdx + 1)
	return ch.slice(idx)
}

func (ch *AckByteChannel) Ack(cb func([]byte) bool) (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for ch.toAck() {
		idx := ch.head.startIdx

		if !cb(ch.slice(idx)) {
			break
		}

		ch.head.startIdx = ch.index(1)
		ch.head.length--
		count++
	}

	if count > 0 {
		ch.writeCond.Broadcast()
	}

	return
}

func (ch *AckByteChannel) AckAll() (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.toAck() {
		return
	}

	count = ch.awaitingAck()
	ch.head.startIdx = ch.head.cursorIdx

	if count > 0 {
		ch.writeCond.Broadcast()
	}

	return
}

func (ch *AckByteChannel) ReadAndAckOrBlock() (b []byte, ok bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Wait until there is data in the buffer
	for !ch.toRead() {
		if ch.closedWriting {
			return
		}

		ch.readCond.Wait()
	}

	idx := ch.head.cursorIdx
	ch.head.cursorIdx = ch.wrap(ch.head.cursorIdx + 1)
	ch.head.startIdx = ch.head.cursorIdx
	ch.head.length--
	return ch.slice(idx), true
}

func (ch *AckByteChannel) ReadAndAckOrFail() (b []byte, ok bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.toRead() {
		return
	}

	idx := ch.head.cursorIdx
	ch.head.cursorIdx = ch.wrap(ch.head.cursorIdx + 1)
	ch.head.startIdx = ch.head.cursorIdx
	ch.head.length--
	return ch.slice(idx), true
}

func (ch *AckByteChannel) Flush() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.flush()
}

func (ch *AckByteChannel) flush() error {
	return ch.data.Flush()
}

func (ch *AckByteChannel) CloseWriting() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.closedWriting {
		ch.closedWriting = true
		ch.readCond.Signal()
	}
}

func (ch *AckByteChannel) Close() (err error) {
	ch.CloseWriting()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.closedWriting = true

	if err = ch.flush(); err != nil {
		return
	}

	return ch.file.Close()
}

func (ch *AckByteChannel) Rewind() (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	count = ch.awaitingAck()
	ch.head.cursorIdx = ch.head.startIdx

	if count > 0 {
		ch.readCond.Broadcast()
	}

	return
}

func (ch *AckByteChannel) ToRead() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.toRead()
}

func (ch *AckByteChannel) toRead() bool {
	return ch.unread() > 0
}

func (ch *AckByteChannel) ToAck() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.toAck()
}

func (ch *AckByteChannel) toAck() bool {
	return ch.awaitingAck() > 0
}

func (ch *AckByteChannel) spaceLeft() bool {
	return ch.head.length < ch.head.capacity
}

func (ch *AckByteChannel) Len() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.head.length
}

func (ch *AckByteChannel) Unread() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.unread()
}

func (ch *AckByteChannel) unread() int64 {
	return ch.indexDiff(ch.head.cursorIdx, ch.endIdx())
}

func (ch *AckByteChannel) AwaitingAck() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return int(ch.awaitingAck())
}

func (ch *AckByteChannel) awaitingAck() int64 {
	return ch.head.length - ch.unread()
}

func (ch *AckByteChannel) Reset() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.head.startIdx = 0
	ch.head.cursorIdx = 0
	ch.head.length = 0
	ch.writeCond.Broadcast()
}

func (ch *AckByteChannel) slice(index int64) []byte {
	index *= ch.head.itemSize
	index += ch.head.headSize
	return ch.data[index : index+ch.head.itemSize]
}

func (ch *AckByteChannel) endIdx() int64 {
	return ch.index(ch.head.length)
}

func (ch *AckByteChannel) index(index int64) int64 {
	return ch.wrap(ch.head.startIdx + index)
}

func (ch *AckByteChannel) indexDiff(left, right int64) int64 {
	return ch.wrap(right - left)
}

func (ch *AckByteChannel) wrap(index int64) int64 {
	return (index + ch.head.capacity) % ch.head.capacity
}
