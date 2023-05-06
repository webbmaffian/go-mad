package hashmmap

import (
	"github.com/webbmaffian/go-mad/internal/utils"
)

type Iterator[K utils.Unsigned, V any] struct {
	raw     *Raw[K, V]
	link    *Link[K, V]
	bucket  K
	nextIdx K
}

func (iter *Iterator[K, V]) Next() bool {
	// log.Println(iter.bucket)
	if iter.nextIdx == 0 {
		if iter.bucket < iter.raw.head.buckets-1 {
			iter.bucket++
			iter.nextIdx = *iter.raw.getIndexAtIndex(iter.raw.getBucketIdx(iter.bucket))
			return iter.Next()
		}

		return false
	}

	iter.link = iter.raw.getLinkAtIndex(iter.nextIdx)
	iter.nextIdx = iter.link.NextIdx

	return true
}

func (iter *Iterator[K, V]) Key() K {
	return iter.link.Key
}

func (iter *Iterator[K, V]) Val() *V {
	return &iter.link.Val
}
