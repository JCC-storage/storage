package efile

import (
	"fmt"
	"net/url"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/http2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type ECMultiplier struct {
	token     string
	url       string
	feat      *cdssdk.ECMultiplierFeature
	outputs   []string
	completed bool
}

// 进行EC运算，coef * inputs。coef为编码矩阵，inputs为待编码数据，chunkSize为分块大小。
// 输出为每一个块文件的路径，数组长度 = len(coef)
func (m *ECMultiplier) Multiply(coef [][]byte, inputs []types.HTTPReqeust, chunkSize int64) ([]string, error) {
	type Request struct {
		Inputs    []types.HTTPReqeust `json:"inputs"`
		Outputs   []string            `json:"outputs"`
		Coefs     [][]byte            `json:"coefs"`
		ChunkSize int64               `json:"chunkSize"`
	}
	type Response struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}

	fileName := os2.GenerateRandomFileName(10)
	m.outputs = make([]string, len(coef))
	for i := range m.outputs {
		m.outputs[i] = fmt.Sprintf("%s_%d", fileName, i)
	}

	u, err := url.JoinPath(m.url, "efile/openapi/v2/createECTask")
	if err != nil {
		return nil, err
	}

	resp, err := http2.PostJSON(u, http2.RequestParam{
		Header: map[string]string{"token": m.token},
		Body: Request{
			Inputs:    inputs,
			Outputs:   m.outputs,
			Coefs:     coef,
			ChunkSize: chunkSize,
		},
	})
	if err != nil {
		return nil, err
	}

	var r Response
	err = serder.JSONToObjectStream(resp.Body, &r)
	if err != nil {
		return nil, err
	}

	if r.Code != "0" {
		return nil, fmt.Errorf("code: %s, msg: %s", r.Code, r.Msg)
	}

	return m.outputs, nil
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

		for _, output := range m.outputs {
			http2.PostJSON(u, http2.RequestParam{
				Header: map[string]string{"token": m.token},
				Query:  map[string]string{"paths": output},
			})
		}
	}
}
