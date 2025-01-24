package efile

import (
	"fmt"
	"net/http"
	"net/url"
	"path"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/http2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type ECMultiplier struct {
	blder     *builder
	url       string
	feat      *cdssdk.ECMultiplierFeature
	outputs   []string
	completed bool
}

// 进行EC运算，coef * inputs。coef为编码矩阵，inputs为待编码数据，chunkSize为分块大小。
// 输出为每一个块文件的路径，数组长度 = len(coef)
func (m *ECMultiplier) Multiply(coef [][]byte, inputs []types.HTTPRequest, chunkSize int) ([]types.BypassUploadedFile, error) {
	type Request struct {
		Inputs    []types.HTTPRequest `json:"inputs"`
		Outputs   []string            `json:"outputs"`
		Coefs     [][]int             `json:"coefs"` // 用int防止被base64编码
		ChunkSize int                 `json:"chunkSize"`
	}
	type Response struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Size   int64  `json:"size"`
			Sha256 string `json:"sha256"`
		}
	}

	intCoefs := make([][]int, len(coef))
	for i := range intCoefs {
		intCoefs[i] = make([]int, len(coef[i]))
		for j := range intCoefs[i] {
			intCoefs[i][j] = int(coef[i][j])
		}
	}

	fileName := os2.GenerateRandomFileName(10)
	m.outputs = make([]string, len(coef))
	for i := range m.outputs {
		m.outputs[i] = path.Join(m.feat.TempDir, fmt.Sprintf("%s_%d", fileName, i))
	}

	u, err := url.JoinPath(m.url, "efile/openapi/v2/file/createECTask")
	if err != nil {
		return nil, err
	}

	token, err := m.blder.getToken()
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}

	resp, err := http2.PostJSON(u, http2.RequestParam{
		Header: map[string]string{"token": token},
		Body: Request{
			Inputs:    inputs,
			Outputs:   m.outputs,
			Coefs:     intCoefs,
			ChunkSize: chunkSize,
		},
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	var r Response
	err = serder.JSONToObjectStream(resp.Body, &r)
	if err != nil {
		return nil, err
	}

	if r.Code != "0" {
		return nil, fmt.Errorf("code: %s, msg: %s", r.Code, r.Msg)
	}

	if len(r.Data) != len(m.outputs) {
		return nil, fmt.Errorf("data length not match outputs length")
	}

	ret := make([]types.BypassUploadedFile, len(r.Data))
	for i, data := range r.Data {
		ret[i] = types.BypassUploadedFile{
			Path: m.outputs[i],
			Size: data.Size,
			Hash: cdssdk.NewFullHashFromString(data.Sha256),
		}
	}

	return ret, nil
}

// 完成计算
func (m *ECMultiplier) Complete() {
	m.completed = true
}

// 取消计算。如果已经调用了Complete，则应该无任何影响
func (m *ECMultiplier) Abort() {
	if !m.completed {
		u, err := url.JoinPath(m.url, "efile/openapi/v2/file/remove")
		if err != nil {
			return
		}

		token, err := m.blder.getToken()
		if err != nil {
			return
		}

		for _, output := range m.outputs {
			http2.PostJSON(u, http2.RequestParam{
				Header: map[string]string{"token": token},
				Query:  map[string]string{"paths": output},
			})
		}
	}
}
