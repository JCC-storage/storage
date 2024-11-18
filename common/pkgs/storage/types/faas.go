package types

import "context"

type InternalFaaSCall interface {
	StorageComponent
	GalMultiply(ctx context.Context, coef [][]byte, inputs []string, outputs []string, chunkSize int) error
}
