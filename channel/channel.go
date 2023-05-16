package channel

// TODO

// type Channel struct {
// 	file       *os.File
// 	mem        mmap.MMap
// 	capacity   int
// 	readIndex  uint64
// 	writeIndex uint64
// 	readCond   *sync.Cond
// 	writeCond  *sync.Cond
// }

// func NewChannel(capacity int) (*Channel, error) {
// 	// open the file and map it to memory
// 	file, err := os.CreateTemp("", "mapped-channel-")
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()
// 	err = file.Truncate(int64(capacity))
// 	if err != nil {
// 		return nil, err
// 	}
// 	mem, err := mmap(file.Fd(), 0, capacity, mmap.READ|mmap.WRITE, mmap.ANONymous)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// create the channel object
// 	ch := &Channel{
// 		file:       file,
// 		mem:        mem,
// 		capacity:   capacity,
// 		readIndex:  0,
// 		writeIndex: 0,
// 		readCond:   sync.NewCond(&sync.Mutex{}),
// 		writeCond:  sync.NewCond(&sync.Mutex{}),
// 	}

// 	return ch, nil
// }

// func (ch *Channel) Send(data []byte) error {
// 	ch.writeCond.L.Lock()
// 	defer ch.writeCond.L.Unlock()

// 	// wait until there is space in the buffer
// 	for uint64(len(data)) > (ch.capacity - ch.writeIndex) {
// 		ch.writeCond.Wait()
// 	}

// 	// write the data to the buffer
// 	copy(ch.mem[ch.writeIndex:], data)
// 	ch.writeIndex += uint64(len(data))

// 	// signal the readers
// 	ch.readCond.Signal()

// 	return nil
// }

// func (ch *Channel) Receive() ([]byte, error) {
// 	ch.readCond.L.Lock()
// 	defer ch.readCond.L.Unlock()

// 	// wait until there is data in the buffer
// 	for ch.readIndex == ch.writeIndex {
// 		ch.readCond.Wait()
// 	}

// 	// read the data from the buffer
// 	data := make([]byte, ch.writeIndex-ch.readIndex)
// 	copy(data, ch.mem[ch.readIndex:ch.writeIndex])
// 	ch.readIndex = ch.writeIndex

// 	// signal the writers
// 	ch.writeCond.Signal()

// 	return data, nil
// }

// func (ch *Channel) Close() error {
// 	// unmap the memory and close the file
// 	err := munmap(ch.mem)
// 	if err != nil {
// 		return err
// 	}
// 	return ch.file.Close()
// }
