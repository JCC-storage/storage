package types

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type OpenOption struct {
	FileHash cdssdk.FileHash
	Offset   int64
	Length   int64
}

func NewOpen(fileHash cdssdk.FileHash) OpenOption {
	return OpenOption{
		FileHash: fileHash,
		Offset:   0,
		Length:   -1,
	}
}

func (o *OpenOption) WithLength(len int64) OpenOption {
	o.Length = len
	return *o
}

// [start, end]，即包含end
func (o *OpenOption) WithRange(start int64, end int64) OpenOption {
	o.Offset = start
	o.Length = end - start + 1
	return *o
}

func (o *OpenOption) WithNullableLength(offset int64, length *int64) {
	o.Offset = offset
	if length != nil {
		o.Length = *length
	}
}

func (o *OpenOption) String() string {
	rangeStart := ""
	if o.Offset > 0 {
		rangeStart = fmt.Sprintf("%d", o.Offset)
	}

	rangeEnd := ""
	if o.Length >= 0 {
		rangeEnd = fmt.Sprintf("%d", o.Offset+o.Length-1)
	}

	if rangeStart == "" && rangeEnd == "" {
		return string(o.FileHash)
	}

	return fmt.Sprintf("%s[%s:%s]", string(o.FileHash), rangeStart, rangeEnd)
}
