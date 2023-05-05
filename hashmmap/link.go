package hashmmap

import "github.com/webbmaffian/go-mad/internal/utils"

type Link[K utils.Unsigned, V any] struct {
	NextIdx K
	Key     K
	Val     V
}
