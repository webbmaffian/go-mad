package hashmmap

import (
	"errors"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/webbmaffian/go-mad/internal/utils"
)

func NewRaw[K utils.Unsigned, V any](filepath string, capacity ...K) (m *Raw[K, V], err error) {
	m = &Raw[K, V]{
		head: newHashmmapHeader[K, V](),
	}

	if m.head.valSize == 0 {
		return nil, errors.New("value must be at least 1 byte")
	}

	if capacity != nil && capacity[0] > 0 {
		m.head.capacity = capacity[0]

		if m.head.capacity > m.head.buckets {
			m.head.buckets = m.head.capacity
		}
	}

	var created bool
	info, err := os.Stat(filepath)

	if err == nil {
		if m.file, err = os.OpenFile(filepath, os.O_RDWR, 0); err != nil {
			return
		}

		if err = m.validateHead(info.Size()); err != nil {
			return
		}
	} else if os.IsNotExist(err) {
		if m.file, err = os.Create(filepath); err != nil {
			return
		}

		if err = m.file.Truncate(int64(m.head.fileSize())); err != nil {
			return
		}

		created = true
	} else {
		return
	}

	if m.data, err = mmap.Map(m.file, mmap.RDWR, 0); err != nil {
		return
	}

	if created {
		if copy(m.data[:m.head.headSize], utils.PointerToBytes(m.head, int(m.head.headSize))) != int(m.head.headSize) {
			return nil, errors.New("failed to write header")
		}

		if err = m.Flush(); err != nil {
			return
		}
	}

	m.head = utils.BytesToPointer[hashmmapHeader[K]](m.data[:m.head.headSize])

	var v V
	var val any = v

	_, m.keyed = val.(Keyed[K])

	return
}

func OpenRawRO[K utils.Unsigned, V any](filepath string) (m *Raw[K, V], err error) {
	m = &Raw[K, V]{
		head: newHashmmapHeader[K, V](),
	}

	if m.head.valSize == 0 {
		return nil, errors.New("value must be at least 1 byte")
	}

	info, err := os.Stat(filepath)

	if err != nil {
		return
	}

	if m.file, err = os.OpenFile(filepath, os.O_RDONLY, 0); err != nil {
		return
	}

	if err = m.validateHead(info.Size()); err != nil {
		return
	}

	if m.data, err = mmap.Map(m.file, mmap.RDONLY, 0); err != nil {
		return
	}

	m.head = utils.BytesToPointer[hashmmapHeader[K]](m.data[:m.head.headSize])

	return
}

// Memory-mapped hashmap
type Raw[K utils.Unsigned, V any] struct {
	data  mmap.MMap
	file  *os.File
	head  *hashmmapHeader[K]
	keyed bool
}

func (m *Raw[K, V]) validateHead(fileSize int64) (err error) {
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

	head := utils.BytesToPointer[hashmmapHeader[K]](b)

	if head.keySize != m.head.keySize {
		return errors.New("invalid key size")
	}

	if head.valSize != m.head.valSize {
		return errors.New("invalid value size")
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

func (m *Raw[K, V]) Flush() error {
	return m.data.Flush()
}

func (m *Raw[K, V]) Close() (err error) {
	if err = m.data.Unmap(); err != nil {
		return
	}

	return m.file.Close()
}

func (m *Raw[K, V]) Cap() int {
	return int(m.head.capacity)
}

func (m *Raw[K, V]) Len() int {
	return int(m.head.length)
}

func (m *Raw[K, V]) Count(key K) (count int) {
	iter := m.Find(key)

	for iter.Next() {
		count++
	}

	return
}

func (m *Raw[K, V]) Get(key K) (val V, ok bool) {
	if !m.keyed {
		return
	}

	f := m.Find(key)

	for f.Next() {
		var v any = f.link.Val

		if ok = v.(Keyed[K]).Key() == key; ok {
			val = f.link.Val
			break
		}
	}

	return
}

func (m *Raw[K, V]) Find(key K) Finder[K, V] {
	return Finder[K, V]{
		raw:     m,
		key:     key,
		nextIdx: *m.getIndexAtIndex(m.getBucketIdx(m.getBucket(key))),
	}
}

func (m *Raw[K, V]) Iterate() Iterator[K, V] {
	return Iterator[K, V]{
		raw:     m,
		nextIdx: *m.getIndexAtIndex(m.getBucketIdx(0)),
	}
}

func (m *Raw[K, V]) Add(key K, val V) {
	idx := m.getAvailableIndex()
	link := m.getLinkAtIndex(idx)
	link.Key, link.Val, link.NextIdx = key, val, 0
	*m.findLeafIdx(key) = idx
	m.head.length++
}

func (m *Raw[K, V]) findLeafIdx(key K) (idx *K) {
	idx = m.getIndexAtIndex(m.getBucketIdx(m.getBucket(key)))

	for *idx != 0 {
		idx = m.getIndexAtIndex(*idx)
	}

	return
}

func (m *Raw[K, V]) getBucket(key K) K {
	return key % m.head.buckets
}

func (m *Raw[K, V]) getBucketIdx(bucket K) (idx K) {
	return m.head.headSize + bucket*m.head.keySize
}

func (m *Raw[K, V]) getIndexAtIndex(idx K) *K {
	return utils.BytesToPointer[K](m.data[idx : idx+m.head.keySize])
}

func (m *Raw[K, V]) getLinkAtIndex(idx K) *Link[K, V] {
	return utils.BytesToPointer[Link[K, V]](m.data[idx : idx+m.head.linkSize])
}

func (m *Raw[K, V]) getAvailableIndex() (idx K) {
	return m.head.headSize + m.head.buckets*m.head.keySize + m.head.length*m.head.linkSize
}
