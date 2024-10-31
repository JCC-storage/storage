package obs

type OBSClient struct {
}

func (c *OBSClient) InitiateMultipartUpload(objectName string) (string, error) {
	return "", nil
}

func (c *OBSClient) UploadPart() {

}

func (c *OBSClient) CompleteMultipartUpload() (string, error) {
	return "", nil
}

func (c *OBSClient) AbortMultipartUpload() {

}

func (c *OBSClient) Close() {

}
