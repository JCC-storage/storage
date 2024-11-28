package types

import "io"

type BypassWriter interface {
	Write(stream io.Reader) (string, error)
}
