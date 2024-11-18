package types

import "io"

type BypassWriter interface {
	StorageComponent
	Write(stream io.Reader) (string, error)
}
