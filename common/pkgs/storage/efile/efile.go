package efile

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/http2"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.EFileType](func(detail stgmod.StorageDetail) types.StorageBuilder {
		return &builder{
			detail: detail,
		}
	})
}

type builder struct {
	types.EmptyBuilder
	detail       stgmod.StorageDetail
	token        string
	tokenLock    sync.Mutex
	getTokenTime time.Time
}

func (b *builder) getToken() (string, error) {
	stgType := b.detail.Storage.Type.(*cdssdk.EFileType)

	b.tokenLock.Lock()
	defer b.tokenLock.Unlock()

	if b.token != "" {
		dt := time.Since(b.getTokenTime)
		if dt < time.Second*time.Duration(stgType.TokenExpire) {
			return b.token, nil
		}
	}

	u, err := url.JoinPath(stgType.TokenURL, "/ac/openapi/v2/tokens")
	if err != nil {
		return "", err
	}

	resp, err := http2.PostJSON(u, http2.RequestParam{
		Header: map[string]string{
			"user":     stgType.User,
			"password": stgType.Password,
			"orgId":    stgType.OrgID,
		},
	})
	if err != nil {
		return "", err
	}

	type Response struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			ClusterID string `json:"clusterId"`
			Token     string `json:"token"`
		} `json:"data"`
	}

	var r Response
	err = serder.JSONToObjectStream(resp.Body, &r)
	if err != nil {
		return "", err
	}

	if r.Code != "0" {
		return "", fmt.Errorf("code:%s, msg:%s", r.Code, r.Msg)
	}

	for _, d := range r.Data {
		if d.ClusterID == stgType.ClusterID {
			b.token = d.Token
			b.getTokenTime = time.Now()
			return d.Token, nil
		}
	}

	return "", fmt.Errorf("clusterID %s not found", stgType.ClusterID)
}

func (b *builder) CreateECMultiplier() (types.ECMultiplier, error) {
	feat := utils.FindFeature[*cdssdk.ECMultiplierFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature ECMultiplier not found")
	}

	return &ECMultiplier{
		blder: b,
		url:   b.detail.Storage.Type.(*cdssdk.EFileType).APIURL,
		feat:  feat,
	}, nil
}
