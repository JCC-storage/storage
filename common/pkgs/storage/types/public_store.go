package types

import (
	"io"
)

type PublicStore interface {
	Start(ch *StorageEventChan)
	Stop()

	Write(objectPath string, stream io.Reader) error
}
