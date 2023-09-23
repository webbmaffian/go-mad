package channel

import (
	"context"
	"sync"
)

type MemoryByteChannel struct {
	data          []byte
	readCond      sync.Cond // Awaited by readers, notified by writers.
	writeCond     sync.Cond // Awaited by writers, notified by readers.
	mu            sync.Mutex
	itemSize      int64
	startIdx      int64
	length        int64
	capacity      int64
	itemsWritten  uint64
	itemsRead     uint64
	closedWriting bool
}

func NewMemoryByteChannel(capacity int, itemSize int) (ch *MemoryByteChannel) {
	ch = &MemoryByteChannel{
		data:     make([]byte, itemSize*capacity),
		itemSize: int64(itemSize),
		capacity: int64(capacity),
	}

	ch.readCond.L = &ch.mu
	ch.writeCond.L = &ch.mu

	return
}

func (ch *MemoryByteChannel) CopyTo(dst *MemoryByteChannel) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	dst.mu.Lock()
	defer dst.mu.Unlock()

	var i int64

	for i = 0; i < ch.length; i++ {
		idx := ch.index(i)

		dst.write(func(b []byte) {
			copy(b, ch.slice(idx))
		})
	}

	if ch.length < dst.capacity {
		dst.length = ch.length
	} else {
		dst.length = dst.capacity
	}
}

func (ch *MemoryByteChannel) WriteOrBlock(cb func([]byte)) bool {
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

func (ch *MemoryByteChannel) WriteOrFail(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting || !ch.spaceLeft() {
		return false
	}

	ch.write(cb)
	return true
}

func (ch *MemoryByteChannel) WriteOrReplace(cb func([]byte)) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closedWriting {
		return false
	}

	ch.write(cb)
	return true
}

func (ch *MemoryByteChannel) write(cb func([]byte)) {
	idx := ch.index(ch.length)
	cb(ch.slice(idx))

	if ch.spaceLeft() {
		ch.length++
	} else {
		ch.startIdx = ch.index(1)
	}

	ch.itemsWritten++
	ch.readCond.Broadcast()
}

func (ch *MemoryByteChannel) Wait() (ok bool) {
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

func (ch *MemoryByteChannel) ReadToCallback(cb func([]byte) error, undoOnError bool) (err error) {
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

func (ch *MemoryByteChannel) WaitForSync(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // Ensure that the child context is canceled when we're done

	// This goroutine waits for the context to be done and then signals the main goroutine.
	go func() {
		<-ctx.Done()
		ch.writeCond.Broadcast() // wake up the main goroutine
	}()

	ch.mu.Lock()
	for !ch.empty() && ctx.Err() == nil {
		ch.writeCond.Wait() // wait either for items to be read or for the context to be done
	}
	ch.mu.Unlock()

	// If the context was cancelled or timed out, return its error.
	return ctx.Err()
}

func (ch *MemoryByteChannel) read() []byte {
	idx := ch.index(0)
	ch.length--
	ch.itemsRead++

	if ch.length > 0 {
		ch.startIdx = ch.index(1)
	}

	return ch.slice(idx)
}

func (ch *MemoryByteChannel) undoRead() {
	ch.startIdx = ch.index(-1)
	ch.length++
	ch.itemsRead--
}

func (ch *MemoryByteChannel) Flush() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.flush()
}

func (ch *MemoryByteChannel) flush() error {
	// TODO
	return nil
}

func (ch *MemoryByteChannel) CloseWriting() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.closedWriting {
		ch.closedWriting = true
		ch.writeCond.Broadcast()
	}
}

func (ch *MemoryByteChannel) Close() (err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.closedWriting = true

	return ch.flush()
}

func (ch *MemoryByteChannel) Empty() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.empty()
}

func (ch *MemoryByteChannel) empty() bool {
	return ch.len() <= 0
}

func (ch *MemoryByteChannel) SpaceLeft() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.spaceLeft()
}

func (ch *MemoryByteChannel) spaceLeft() bool {
	return ch.len() < ch.cap()
}

func (ch *MemoryByteChannel) Len() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.len()
}

func (ch *MemoryByteChannel) len() int64 {
	return ch.length
}

func (ch *MemoryByteChannel) Cap() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.cap()
}

func (ch *MemoryByteChannel) cap() int64 {
	return ch.capacity
}

func (ch *MemoryByteChannel) Reset() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.startIdx = 0
	ch.length = 0
	ch.writeCond.Broadcast()
}

func (ch *MemoryByteChannel) ItemsWritten() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.itemsWritten
}

func (ch *MemoryByteChannel) ItemsRead() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.itemsRead
}

func (ch *MemoryByteChannel) slice(index int64) []byte {
	index *= ch.itemSize
	return ch.data[index : index+ch.itemSize]
}

func (ch *MemoryByteChannel) index(index int64) int64 {
	return ch.wrap(ch.startIdx + index)
}

func (ch *MemoryByteChannel) wrap(index int64) int64 {
	return (index + ch.capacity) % ch.capacity
}
