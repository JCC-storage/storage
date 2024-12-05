package s3

import (
	"encoding/base64"
	"fmt"
	"strings"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func JoinKey(comps ...string) string {
	sb := strings.Builder{}

	hasTrailingSlash := true
	for _, comp := range comps {
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

func DecodeBase64Hash(hash string) (cdssdk.FileHash, error) {
	hashBytes := make([]byte, 32)
	n, err := base64.RawStdEncoding.Decode(hashBytes, []byte(hash))
	if err != nil {
		return "", err
	}
	if n != 32 {
		return "", fmt.Errorf("invalid hash length: %d", n)
	}

	return cdssdk.FileHash(strings.ToUpper(string(hashBytes))), nil
}
