package types

import (
	"io"
)

type SharedStore interface {
	Start(ch *StorageEventChan)
	Stop()

	Write(objectPath string, stream io.Reader) error
}
