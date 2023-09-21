package channel

import (
	"errors"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

type AckByteChannelReadonly struct {
	data mmap.MMap
	file *os.File
	head *header
}

func OpenAckByteChannelReadonly(filepath string) (ch *AckByteChannelReadonly, err error) {
	ch = &AckByteChannelReadonly{
		head: newHeader(0, 0),
	}

	info, err := os.Stat(filepath)

	if err != nil {
		return
	}

	if ch.file, err = os.OpenFile(filepath, os.O_RDWR, 0); err != nil {
		return
	}

	if err = ch.validateHead(info.Size()); err != nil {
		return
	}

	if ch.data, err = mmap.Map(ch.file, mmap.RDWR, 0); err != nil {
		return
	}

	ch.head = utils.BytesToPointer[header](ch.data[:ch.head.headSize])

	return
}

func (ch *AckByteChannelReadonly) validateHead(fileSize int64) (err error) {
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
	if head.awaitingAck > head.length {
		return errors.New("invalid awaiting ack")
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

func (ch *AckByteChannelReadonly) StartIndex() int64 {
	return ch.head.startIdx
}

func (ch *AckByteChannelReadonly) Cap() int64 {
	return ch.head.capacity
}

func (ch *AckByteChannelReadonly) ItemSize() int64 {
	return ch.head.itemSize
}

func (ch *AckByteChannelReadonly) Peek(index int64) []byte {
	return ch.peek(index)
}

func (ch *AckByteChannelReadonly) peek(index int64) []byte {
	return ch.slice(index)
}

func (ch *AckByteChannelReadonly) Close() (err error) {
	return ch.file.Close()
}

func (ch *AckByteChannelReadonly) Len() int64 {
	return ch.head.length
}

func (ch *AckByteChannelReadonly) Unread() int64 {
	return ch.unread()
}

func (ch *AckByteChannelReadonly) unread() int64 {
	return ch.head.length - ch.head.awaitingAck
}

func (ch *AckByteChannelReadonly) AwaitingAck() int64 {
	return ch.head.awaitingAck
}

func (ch *AckByteChannelReadonly) MsgWritten() uint64 {
	return ch.head.written
}

func (ch *AckByteChannelReadonly) MsgRead() uint64 {
	return ch.head.read
}

func (ch *AckByteChannelReadonly) slice(index int64) []byte {
	index *= ch.head.itemSize
	index += ch.head.headSize
	return ch.data[index : index+ch.head.itemSize]
}

func (ch *AckByteChannelReadonly) index(index int64) int64 {
	return ch.wrap(ch.head.startIdx + index)
}

func (ch *AckByteChannelReadonly) indexDiff(left, right int64) int64 {
	return ch.wrap(right - left)
}

func (ch *AckByteChannelReadonly) wrap(index int64) int64 {
	return (index + ch.head.capacity) % ch.head.capacity
}
