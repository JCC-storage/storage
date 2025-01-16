package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
)

const (
	AuthRegion          = "any"
	AuthService         = "jcs"
	AuthorizationHeader = "Authorization"
)

type AWSAuth struct {
	cred   aws.Credentials
	signer *v4.Signer
}

func NewAWSAuth(accessKey string, secretKey string) (*AWSAuth, error) {
	prod := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	cred, err := prod.Retrieve(context.TODO())
	if err != nil {
		return nil, err
	}

	return &AWSAuth{
		cred:   cred,
		signer: v4.NewSigner(),
	}, nil
}

func (a *AWSAuth) Auth(c *gin.Context) {
	authorizationHeader := c.GetHeader(AuthorizationHeader)
	if authorizationHeader == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.Unauthorized, "authorization header is missing"))
		return
	}

	_, headers, reqSig, err := parseAuthorizationHeader(authorizationHeader)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.Unauthorized, "invalid Authorization header format"))
		return
	}

	// 限制请求体大小
	rd := io.LimitReader(c.Request.Body, config.Cfg().MaxHTTPBodySize)
	body, err := io.ReadAll(rd)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "read request body failed"))
		return
	}

	payloadHash := sha256.Sum256(body)
	hexPayloadHash := hex.EncodeToString(payloadHash[:])

	// 构造验签用的请求
	verifyReq, err := http.NewRequest(c.Request.Method, c.Request.URL.String(), nil)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.OperationFailed, err.Error()))
		return
	}
	for _, h := range headers {
		verifyReq.Header.Add(h, c.Request.Header.Get(h))
	}
	verifyReq.Host = c.Request.Host

	timestamp, err := time.Parse("20060102T150405Z", c.GetHeader("X-Amz-Date"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid X-Amz-Date header format"))
		return
	}

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.TODO(), a.cred, verifyReq, hexPayloadHash, AuthService, AuthRegion, timestamp)
	if err != nil {
		logger.Warnf("sign request: %v", err)
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.OperationFailed, "sign request failed"))
		return
	}

	verifySig := a.getSignature(verifyReq)
	if !strings.EqualFold(verifySig, reqSig) {
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.Unauthorized, "signature mismatch"))
		return
	}

	c.Request.Body = io.NopCloser(bytes.NewReader(body))

	c.Next()
}

func (a *AWSAuth) AuthWithoutBody(c *gin.Context) {
	authorizationHeader := c.GetHeader(AuthorizationHeader)
	if authorizationHeader == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.Unauthorized, "authorization header is missing"))
		return
	}

	_, headers, reqSig, err := parseAuthorizationHeader(authorizationHeader)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.Unauthorized, "invalid Authorization header format"))
		return
	}

	// 构造验签用的请求
	verifyReq, err := http.NewRequest(c.Request.Method, c.Request.URL.String(), nil)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.OperationFailed, err.Error()))
		return
	}
	for _, h := range headers {
		verifyReq.Header.Add(h, c.Request.Header.Get(h))
	}
	verifyReq.Host = c.Request.Host

	timestamp, err := time.Parse("20060102T150405Z", c.GetHeader("X-Amz-Date"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid X-Amz-Date header format"))
		return
	}

	err = a.signer.SignHTTP(context.TODO(), a.cred, verifyReq, "", AuthService, AuthRegion, timestamp)
	if err != nil {
		logger.Warnf("sign request: %v", err)
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.OperationFailed, "sign request failed"))
		return
	}

	verifySig := a.getSignature(verifyReq)
	if strings.EqualFold(verifySig, reqSig) {
		c.AbortWithStatusJSON(http.StatusOK, Failed(errorcode.Unauthorized, "signature mismatch"))
		return
	}

	c.Next()
}

// 解析 Authorization 头部
func parseAuthorizationHeader(authorizationHeader string) (string, []string, string, error) {
	if !strings.HasPrefix(authorizationHeader, "AWS4-HMAC-SHA256 ") {
		return "", nil, "", fmt.Errorf("invalid Authorization header format")
	}

	authorizationHeader = strings.TrimPrefix(authorizationHeader, "AWS4-HMAC-SHA256")

	parts := strings.Split(authorizationHeader, ",")
	if len(parts) != 3 {
		return "", nil, "", fmt.Errorf("invalid Authorization header format")
	}

	var credential, signedHeaders, signature string
	for _, part := range parts {
		part = strings.TrimSpace(part)

		if strings.HasPrefix(part, "Credential=") {
			credential = strings.TrimPrefix(part, "Credential=")
		}
		if strings.HasPrefix(part, "SignedHeaders=") {
			signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		}
		if strings.HasPrefix(part, "Signature=") {
			signature = strings.TrimPrefix(part, "Signature=")
		}
	}

	if credential == "" || signedHeaders == "" || signature == "" {
		return "", nil, "", fmt.Errorf("missing necessary parts in Authorization header")
	}

	headers := strings.Split(signedHeaders, ";")
	return credential, headers, signature, nil
}

func (a *AWSAuth) getSignature(req *http.Request) string {
	auth := req.Header.Get(AuthorizationHeader)
	idx := strings.Index(auth, "Signature=")
	if idx == -1 {
		return ""
	}

	return auth[idx+len("Signature="):]
}
