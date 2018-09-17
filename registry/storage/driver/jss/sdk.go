package jss

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	accessKey  string
	secretKey  string
	host       string
	httpClient *http.Client
}

type Bucket struct {
	*Client
	Name string
}

type request struct {
	method     string
	bucketName string
	objectName string
	headers    http.Header
	params     url.Values
	body       []byte
}

type Error struct {
	StatusCode int    // HTTP status code (200, 403, ...)
	Code       string // JSS error code ("UnsupportedOperation", ...)
	Message    string // The human-oriented error message
	Resource   string
	RequestId  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("JSS API Error: RequestId: %s Status Code: %d Code: %s Message: %s Resource: %s", e.RequestId, e.StatusCode, e.Code, e.Message, e.Resource)
}

func NewJSSClient(accessKey string, secretKey string, host string, timeout time.Duration) *Client {
	if strings.HasSuffix(host, "/") {
		host = host[:len(host)-1]
	}

	return &Client{
		accessKey: accessKey,
		secretKey: secretKey,
		host:      host,
		httpClient: &http.Client{
			Transport: &http.Transport{
				Dial: func(netw, addr string) (c net.Conn, err error) {
					return net.DialTimeout(netw, addr, timeout)
				},
				Proxy: http.ProxyFromEnvironment,
			},
			Timeout: timeout,
		},
	}
}

func (b *Bucket) Get(objName string, offset int64) (data []byte, err error) {
	headers := make(http.Header)
	if offset > 0 {
		headers.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	data, _, err = b.Client.doRequest(&request{
		method:     http.MethodGet,
		bucketName: b.Name,
		objectName: objName,
		headers:    headers,
		params:     make(url.Values),
	})
	return data, err
}

func (b *Bucket) Put(objName string, data []byte) error {
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("Content-MD5", getBytesMd5(data))

	_, _, err := b.Client.doRequest(&request{
		method:     http.MethodPut,
		bucketName: b.Name,
		objectName: objName,
		headers:    headers,
		params:     make(url.Values),
		body:       data,
	})
	return err
}

func (b *Bucket) NewMulti(objName string) (*Multi, error) {
	params := make(url.Values)
	params.Set("uploads", "")

	data, _, err := b.Client.doRequest(&request{
		method:     http.MethodPost,
		bucketName: b.Name,
		objectName: objName + "?uploads",
		headers:    make(http.Header),
		params:     params,
	})
	if err != nil || data == nil {
		return nil, err
	}

	var responseJson struct {
		Bucket   string
		Key      string
		UploadId string
	}
	err = json.Unmarshal(data, &responseJson)
	if err != nil {
		return nil, err
	}

	return &Multi{b, responseJson.Key, responseJson.UploadId}, nil
}

func (b *Bucket) ListMulti(prefix, delim string) (multis []*Multi, prefixes []string, err error) {
	params := make(url.Values)
	params.Set("uploads", "")
	params.Set("prefix", prefix)
	params.Set("delimiter", delim)

	data, _, err := b.Client.doRequest(&request{
		method:     http.MethodGet,
		bucketName: b.Name + "?uploads",
		headers:    make(http.Header),
		params:     params,
	})
	if err != nil || data == nil {
		return nil, nil, err
	}

	var listMultiResp struct {
		Bucket             string
		Delimiter          string
		NextKeyMarker      string
		NextUploadIdMarker string
		Upload             []Multi
		CommonPrefixes     []string
	}
	err = json.Unmarshal(data, &listMultiResp)
	if err != nil {
		return nil, nil, err
	}

	for i := range listMultiResp.Upload {
		multi := &listMultiResp.Upload[i]
		multi.Bucket = b
		multis = append(multis, multi)
	}

	return multis, listMultiResp.CommonPrefixes, nil
}

type Delete struct {
	Quiet   bool
	Objects []Object
}

type Object struct {
	Key       string
	VersionId string
}

func (b *Bucket) Del(objName string) error {
	data, _, err := b.Client.doRequest(&request{
		method:     http.MethodDelete,
		bucketName: b.Name,
		objectName: objName,
		headers:    make(http.Header),
		params:     make(url.Values),
	})
	if err != nil || data == nil {
		return err
	}
	return nil
}

func (b *Bucket) DelMulti(objects Delete) error {
	for _, obj := range objects.Objects {
		err := b.Del(obj.Key)

		if err != nil {
			return err
		}
	}

	return nil
}

type ListResp struct {
	Name           string
	Prefix         string
	Delimiter      string
	Marker         string
	MaxKeys        int
	HasNext        bool
	Contents       []Key
	CommonPrefixes []string
	NextMarker     string
}

type Key struct {
	Key          string
	LastModified string
	Size         int64
	ETag         string
}

func (b *Bucket) List(prefix, delim, marker string, max int) (result *ListResp, err error) {
	params := make(url.Values)
	params.Set("prefix", prefix)
	params.Set("delimiter", delim)
	params.Set("marker", marker)
	if max != 0 {
		params["max-keys"] = []string{strconv.FormatInt(int64(max), 10)}
	}

	//req jss
	data, _, err := b.Client.doRequest(&request{
		method:     http.MethodGet,
		bucketName: b.Name,
		headers:    make(http.Header),
		params:     params,
	})

	if err != nil || data == nil {
		return nil, err
	}

	//parse json
	result = &ListResp{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	if result.HasNext && result.NextMarker == "" {
		n := len(result.Contents)
		if n > 0 {
			result.NextMarker = result.Contents[n-1].Key
		}
	}

	return result, nil
}

func (b *Bucket) SignedURLWithMethod(method, path string, expires time.Time, params url.Values, headers http.Header) string {
	if !strings.HasPrefix(b.Name, "/") {
		b.Name = "/" + b.Name
	}

	if path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
	}

	var bs []string
	for k, _ := range headers {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-jss-") {
			bs = append(bs, k+":"+headers.Get(k))
		}
	}
	customHead := strings.Join(bs, "\n")

	Expires := strconv.FormatInt(expires.Unix(), 10)
	h := hmac.New(sha1.New, []byte(b.Client.secretKey))
	resource := b.Name + path
	var param []string
	if len(customHead) > 0 {
		param = []string{method, "", "", Expires, customHead, resource}
	} else {
		param = []string{method, "", "", Expires, resource}
	}
	io.WriteString(h, strings.Join(param, "\n"))
	Signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	endSignature := url.QueryEscape(Signature)
	queryUrl := b.host + b.Name + path + "?Expires=" + Expires + "&AccessKey=" + b.Client.accessKey + "&Signature=" + endSignature
	return queryUrl
}

const (
	X_JSS_COPY_SOURCE = "x-jss-copy-source"
	X_JSS_MOVE_SOURCE = "x-jss-move-source"
)

//dstBucketName, dstObjectName, srcBucketName, srcObjectName string
func (b *Bucket) CopyObj(sourcePath string, destPath string, isMove bool) error {
	moveHeaders := make(http.Header)
	//copySource := fmt.Sprintf("%s%s", b.Name, url.QueryEscape(sourcePath))
	copySource := fmt.Sprintf("/%s/%s", b.Name, sourcePath)
	if isMove {
		moveHeaders.Set(X_JSS_MOVE_SOURCE, copySource)
	} else {
		moveHeaders.Set(X_JSS_COPY_SOURCE, copySource)
	}

	_, _, err := b.Client.doRequest(&request{
		method:     http.MethodPut,
		bucketName: b.Name,
		objectName: destPath,
		headers:    moveHeaders,
	})

	return err
}

type Multi struct {
	Bucket   *Bucket
	Key      string
	UploadId string
}

type Part struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64
}

func (m *Multi) ListParts() ([]Part, error) {
	return m.ListPartsFull(0, listMax)
}

func (m *Multi) ListPartsFull(partNumberMarker int, maxParts int) ([]Part, error) {
	if maxParts > listMax {
		maxParts = listMax
	}

	params := make(url.Values)
	params.Set("uploadId", m.UploadId)
	params.Set("max-parts", strconv.FormatInt(int64(maxParts), 10))
	params.Set("part-number-marker", strconv.FormatInt(int64(partNumberMarker), 10))

	data, _, err := m.Bucket.Client.doRequest(&request{
		method:     http.MethodGet,
		bucketName: m.Bucket.Name,
		objectName: m.Key + "?uploadId=" + m.UploadId,
		headers:    make(http.Header),
		params:     params,
	})
	if err != nil || data == nil {
		return nil, err
	}

	var responseJson struct {
		Bucket               string
		Key                  string
		UploadId             string
		PartNumberMarker     int
		NextPartNumberMarker int
		MaxParts             int
		HasNext              bool
		Part                 []Part
	}
	err = json.Unmarshal(data, &responseJson)
	if err != nil {
		return nil, err
	}

	return responseJson.Part, nil
}

func (m *Multi) PutPart(n int, data []byte) (Part, error) {
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	headers.Set("Content-MD5", getBytesMd5(data))

	params := make(url.Values)
	params.Set("uploadId", m.UploadId)
	params.Set("partNumber", strconv.FormatInt(int64(n), 10))

	respData, respHeader, err := m.Bucket.Client.doRequest(&request{
		method:     http.MethodPut,
		bucketName: m.Bucket.Name,
		objectName: m.Key + "?partNumber=" + strconv.FormatInt(int64(n), 10) + "&uploadId=" + m.UploadId,
		headers:    headers,
		params:     params,
		body:       data,
	})

	if err != nil {
		return Part{}, err
	}
	etag := respHeader.Get("ETag")
	if etag == "" {
		return Part{}, errors.New("part upload succeeded with no ETag")
	}
	return Part{n, "", etag, int64(len(respData))}, nil
}

type completeUpload struct {
	Part completeParts
}

type completePart struct {
	PartNumber int
	ETag       string
}

type completeParts []completePart

func (p completeParts) Len() int           { return len(p) }
func (p completeParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p completeParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (m *Multi) Complete(parts []Part) error {
	c := completeUpload{}
	for _, p := range parts {
		c.Part = append(c.Part, completePart{p.PartNumber, p.ETag})
	}
	sort.Sort(c.Part)
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}

	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	headers.Set("Content-MD5", getBytesMd5(data))
	params := make(url.Values)
	params.Set("uploadId", m.UploadId)

	//req jss
	respData, _, err := m.Bucket.Client.doRequest(&request{
		method:     http.MethodPost,
		bucketName: m.Bucket.Name,
		objectName: m.Key + "?uploadId=" + m.UploadId,
		headers:    headers,
		params:     params,
		body:       data,
	})
	if err != nil {
		return err
	}

	//response parse
	var responseJson struct {
		Bucket string
		Key    string
		ETag   string
	}
	err = json.Unmarshal(respData, &responseJson)
	if err != nil {
		return err
	}

	etag := responseJson.ETag
	if etag == "" {
		return errors.New("part Complete failed!")
	}
	return nil
}

func (m *Multi) Abort() error {
	params := make(url.Values)
	params.Set("uploadId", m.UploadId)

	_, _, err := m.Bucket.Client.doRequest(&request{
		method:     http.MethodDelete,
		bucketName: m.Bucket.Name,
		objectName: m.Key + "?uploadId=" + m.UploadId,
		headers:    make(http.Header),
		params:     params,
	})
	return err
}

func (client *Client) doRequest(req *request) ([]byte, http.Header, error) {
	if !strings.HasPrefix(req.bucketName, "/") {
		req.bucketName = "/" + req.bucketName
	}

	resource := req.bucketName
	if req.objectName != "" {
		if !strings.HasPrefix(req.objectName, "/") {
			req.objectName = "/" + req.objectName
		}
		resource += req.objectName
	}

	date := time.Now().UTC().Format(http.TimeFormat)
	hreq, _ := http.NewRequest(req.method, client.host+resource, bytes.NewReader(req.body))
	hreq.Form = req.params
	hreq.Header = req.headers
	hreq.Header.Add("User-Agent", "Registry-JSS-Client/0.0.1")
	hreq.Header.Set("Connection", "Keep-Alive")
	hreq.Header.Add("Date", date)

	{
		var bs []string
		for k, _ := range req.headers {
			k = strings.ToLower(k)
			if strings.HasPrefix(k, "x-jss-") {
				bs = append(bs, k+":"+req.headers.Get(k))
			}
		}
		customHead := strings.Join(bs, "\n")

		h := hmac.New(sha1.New, []byte(client.secretKey))
		var param []string
		if len(customHead) > 0 {
			param = []string{hreq.Method, hreq.Header.Get("Content-MD5"), hreq.Header.Get("Content-Type"), hreq.Header.Get("Date"), customHead, resource}
		} else {
			param = []string{hreq.Method, hreq.Header.Get("Content-MD5"), hreq.Header.Get("Content-Type"), hreq.Header.Get("Date"), resource}
		}
		io.WriteString(h, strings.Join(param, "\n"))
		sign := "jingdong " + client.accessKey + ":" + base64.StdEncoding.EncodeToString(h.Sum(nil))
		hreq.Header.Set("Authorization", sign)
	}
	hreq.URL.RawQuery = hreq.Form.Encode()

	resp, err := client.httpClient.Do(hreq)
	if err != nil || resp == nil {
		return nil, nil, err
	}

	defer resp.Body.Close()

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.Header, err
	}

	if resp.StatusCode != 200 && resp.StatusCode != 204 && resp.StatusCode != 206 {
		jssErr := Error{}
		jssErr.StatusCode = resp.StatusCode

		json.Unmarshal(respData, &jssErr)

		if jssErr.Message == "" {
			jssErr.Message = resp.Status
		}
		return nil, nil, &jssErr
	}

	return respData, resp.Header, nil
}

func getBytesMd5(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}
