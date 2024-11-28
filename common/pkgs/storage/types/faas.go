package types

import "context"

type InternalFaaSCall interface {
	GalMultiply(ctx context.Context, coef [][]byte, inputs []string, outputs []string, chunkSize int) error
}
