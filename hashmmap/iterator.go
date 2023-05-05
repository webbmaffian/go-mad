package hashmmap

import "github.com/webbmaffian/go-mad/internal/utils"

type Iterator[K utils.Unsigned, V any] struct {
	raw     *Raw[K, V]
	link    *Link[K, V]
	key     K
	nextIdx K
}

func (iter *Iterator[K, V]) Next() bool {
	if iter.nextIdx == 0 {
		return false
	}

	iter.link = iter.raw.getLinkAtIndex(iter.nextIdx)
	iter.nextIdx = iter.link.NextIdx

	if iter.link.Key != iter.key {
		return iter.Next()
	}

	return true
}

func (iter *Iterator[K, V]) Key() K {
	return iter.link.Key
}

func (iter *Iterator[K, V]) Val() *V {
	return &iter.link.Val
}
