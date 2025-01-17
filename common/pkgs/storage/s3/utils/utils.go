package utils

import (
	"encoding/base64"
	"fmt"
	"strings"
)

func JoinKey(comps ...string) string {
	sb := strings.Builder{}

	hasTrailingSlash := true
	for _, comp := range comps {
		if comp == "" {
			continue
		}
		if !hasTrailingSlash {
			sb.WriteString("/")
		}
		sb.WriteString(comp)
		hasTrailingSlash = strings.HasSuffix(comp, "/")
	}

	return sb.String()
}

func BaseKey(key string) string {
	return key[strings.LastIndex(key, "/")+1:]
}

func DecodeBase64Hash(hash string) ([]byte, error) {
	hashBytes := make([]byte, 32)
	n, err := base64.RawStdEncoding.Decode(hashBytes, []byte(hash))
	if err != nil {
		return nil, err
	}
	if n != 32 {
		return nil, fmt.Errorf("invalid hash length: %d", n)
	}

	return hashBytes, nil
}
