package hashmmap

import "github.com/webbmaffian/go-mad/internal/utils"

type Keyed[K utils.Unsigned] interface {
	Key() K
}
