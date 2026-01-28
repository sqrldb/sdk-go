package squirreldb

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// StorageBucket represents a storage bucket
type StorageBucket struct {
	Name      string
	CreatedAt time.Time
}

// StorageObject represents a storage object
type StorageObject struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// MultipartUpload represents a multipart upload
type MultipartUpload struct {
	UploadID string
	Bucket   string
	Key      string
}

// UploadPart represents an uploaded part
type UploadPart struct {
	PartNumber int
	ETag       string
}

// StorageClient is a client for SquirrelDB object storage
type StorageClient struct {
	endpoint  string
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

// StorageOptions configures the storage client
type StorageOptions struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

// NewStorageClient creates a new storage client
func NewStorageClient(opts *StorageOptions) *StorageClient {
	region := opts.Region
	if region == "" {
		region = "us-east-1"
	}
	return &StorageClient{
		endpoint:  strings.TrimRight(opts.Endpoint, "/"),
		accessKey: opts.AccessKey,
		secretKey: opts.SecretKey,
		region:    region,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *StorageClient) signRequest(req *http.Request, payloadHash string) {
	if s.accessKey == "" || s.secretKey == "" {
		return
	}

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	// Create canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalURI = url.PathEscape(canonicalURI)
	canonicalQueryString := req.URL.RawQuery

	// Signed headers
	var signedHeaders []string
	for k := range req.Header {
		signedHeaders = append(signedHeaders, strings.ToLower(k))
	}
	signedHeaders = append(signedHeaders, "host")
	sort.Strings(signedHeaders)
	signedHeadersStr := strings.Join(signedHeaders, ";")

	// Canonical headers
	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		if h == "host" {
			canonicalHeaders.WriteString(fmt.Sprintf("host:%s\n", req.Host))
		} else {
			canonicalHeaders.WriteString(fmt.Sprintf("%s:%s\n", h, req.Header.Get(h)))
		}
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders.String(),
		signedHeadersStr,
		payloadHash,
	}, "\n")

	// String to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)
	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		credentialScope,
		hex.EncodeToString(hash[:]),
	}, "\n")

	// Calculate signature
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(s.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, s.accessKey, credentialScope, signedHeadersStr, signature)
	req.Header.Set("Authorization", authHeader)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// ListBuckets lists all buckets
func (s *StorageClient) ListBuckets(ctx context.Context) ([]StorageBucket, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", s.endpoint+"/", nil)
	if err != nil {
		return nil, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list buckets failed: %s", resp.Status)
	}

	var result struct {
		Buckets struct {
			Bucket []struct {
				Name         string `xml:"Name"`
				CreationDate string `xml:"CreationDate"`
			} `xml:"Bucket"`
		} `xml:"Buckets"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var buckets []StorageBucket
	for _, b := range result.Buckets.Bucket {
		t, _ := time.Parse(time.RFC3339, b.CreationDate)
		buckets = append(buckets, StorageBucket{Name: b.Name, CreatedAt: t})
	}
	return buckets, nil
}

// CreateBucket creates a new bucket
func (s *StorageClient) CreateBucket(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", s.endpoint+"/"+name, nil)
	if err != nil {
		return err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create bucket failed: %s", resp.Status)
	}
	return nil
}

// DeleteBucket deletes a bucket
func (s *StorageClient) DeleteBucket(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", s.endpoint+"/"+name, nil)
	if err != nil {
		return err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete bucket failed: %s", resp.Status)
	}
	return nil
}

// BucketExists checks if a bucket exists
func (s *StorageClient) BucketExists(ctx context.Context, name string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", s.endpoint+"/"+name, nil)
	if err != nil {
		return false, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// ListObjectsOptions configures ListObjects
type ListObjectsOptions struct {
	Prefix  string
	MaxKeys int
}

// ListObjects lists objects in a bucket
func (s *StorageClient) ListObjects(ctx context.Context, bucket string, opts *ListObjectsOptions) ([]StorageObject, error) {
	u := s.endpoint + "/" + bucket
	if opts != nil {
		params := url.Values{}
		if opts.Prefix != "" {
			params.Set("prefix", opts.Prefix)
		}
		if opts.MaxKeys > 0 {
			params.Set("max-keys", fmt.Sprintf("%d", opts.MaxKeys))
		}
		if len(params) > 0 {
			u += "?" + params.Encode()
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list objects failed: %s", resp.Status)
	}

	var result struct {
		Contents []struct {
			Key          string `xml:"Key"`
			Size         int64  `xml:"Size"`
			ETag         string `xml:"ETag"`
			LastModified string `xml:"LastModified"`
		} `xml:"Contents"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var objects []StorageObject
	for _, c := range result.Contents {
		t, _ := time.Parse(time.RFC3339, c.LastModified)
		objects = append(objects, StorageObject{
			Key:          c.Key,
			Size:         c.Size,
			ETag:         strings.Trim(c.ETag, `"`),
			LastModified: t,
		})
	}
	return objects, nil
}

// GetObject gets an object's content
func (s *StorageClient) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", s.endpoint+"/"+bucket+"/"+key, nil)
	if err != nil {
		return nil, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get object failed: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

// GetObjectReader gets an object as an io.ReadCloser
func (s *StorageClient) GetObjectReader(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", s.endpoint+"/"+bucket+"/"+key, nil)
	if err != nil {
		return nil, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("get object failed: %s", resp.Status)
	}

	return resp.Body, nil
}

// PutObjectOptions configures PutObject
type PutObjectOptions struct {
	ContentType string
}

// PutObject uploads an object
func (s *StorageClient) PutObject(ctx context.Context, bucket, key string, data []byte, opts *PutObjectOptions) (string, error) {
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])

	req, err := http.NewRequestWithContext(ctx, "PUT", s.endpoint+"/"+bucket+"/"+key, bytes.NewReader(data))
	if err != nil {
		return "", err
	}

	contentType := "application/octet-stream"
	if opts != nil && opts.ContentType != "" {
		contentType = opts.ContentType
	}

	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	s.signRequest(req, payloadHash)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("put object failed: %s", resp.Status)
	}

	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// DeleteObject deletes an object
func (s *StorageClient) DeleteObject(ctx context.Context, bucket, key string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", s.endpoint+"/"+bucket+"/"+key, nil)
	if err != nil {
		return err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete object failed: %s", resp.Status)
	}
	return nil
}

// CopyObject copies an object
func (s *StorageClient) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "PUT", s.endpoint+"/"+dstBucket+"/"+dstKey, nil)
	if err != nil {
		return "", err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	req.Header.Set("x-amz-copy-source", "/"+srcBucket+"/"+srcKey)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("copy object failed: %s", resp.Status)
	}

	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// ObjectExists checks if an object exists
func (s *StorageClient) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", s.endpoint+"/"+bucket+"/"+key, nil)
	if err != nil {
		return false, err
	}
	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// CreateMultipartUpload initiates a multipart upload
func (s *StorageClient) CreateMultipartUpload(ctx context.Context, bucket, key string, opts *PutObjectOptions) (*MultipartUpload, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", s.endpoint+"/"+bucket+"/"+key+"?uploads", nil)
	if err != nil {
		return nil, err
	}

	contentType := "application/octet-stream"
	if opts != nil && opts.ContentType != "" {
		contentType = opts.ContentType
	}

	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	req.Header.Set("Content-Type", contentType)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create multipart upload failed: %s", resp.Status)
	}

	var result struct {
		UploadId string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &MultipartUpload{
		UploadID: result.UploadId,
		Bucket:   bucket,
		Key:      key,
	}, nil
}

// UploadPart uploads a part
func (s *StorageClient) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data []byte) (*UploadPart, error) {
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])

	u := fmt.Sprintf("%s/%s/%s?partNumber=%d&uploadId=%s", s.endpoint, bucket, key, partNumber, uploadID)
	req, err := http.NewRequestWithContext(ctx, "PUT", u, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	s.signRequest(req, payloadHash)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upload part failed: %s", resp.Status)
	}

	return &UploadPart{
		PartNumber: partNumber,
		ETag:       strings.Trim(resp.Header.Get("ETag"), `"`),
	}, nil
}

// CompleteMultipartUpload completes a multipart upload
func (s *StorageClient) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []UploadPart) (string, error) {
	// Sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Build completion XML
	var buf bytes.Buffer
	buf.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		buf.WriteString(fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.PartNumber, p.ETag))
	}
	buf.WriteString("</CompleteMultipartUpload>")

	body := buf.Bytes()
	hash := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(hash[:])

	u := fmt.Sprintf("%s/%s/%s?uploadId=%s", s.endpoint, bucket, key, uploadID)
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	if err != nil {
		return "", err
	}

	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	s.signRequest(req, payloadHash)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("complete multipart upload failed: %s", resp.Status)
	}

	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// AbortMultipartUpload aborts a multipart upload
func (s *StorageClient) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	u := fmt.Sprintf("%s/%s/%s?uploadId=%s", s.endpoint, bucket, key, uploadID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", u, nil)
	if err != nil {
		return err
	}

	req.Host = strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "http://"), "https://")
	req.Header.Set("Host", req.Host)
	s.signRequest(req, "UNSIGNED-PAYLOAD")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("abort multipart upload failed: %s", resp.Status)
	}
	return nil
}

// UploadLargeObject uploads a large object using multipart upload
func (s *StorageClient) UploadLargeObject(ctx context.Context, bucket, key string, data []byte, partSize int, opts *PutObjectOptions) (string, error) {
	if partSize <= 0 {
		partSize = 5 * 1024 * 1024 // 5MB default
	}

	if len(data) <= partSize {
		return s.PutObject(ctx, bucket, key, data, opts)
	}

	upload, err := s.CreateMultipartUpload(ctx, bucket, key, opts)
	if err != nil {
		return "", err
	}

	var parts []UploadPart
	partNumber := 1
	offset := 0

	for offset < len(data) {
		end := offset + partSize
		if end > len(data) {
			end = len(data)
		}

		part, err := s.UploadPart(ctx, bucket, key, upload.UploadID, partNumber, data[offset:end])
		if err != nil {
			s.AbortMultipartUpload(ctx, bucket, key, upload.UploadID)
			return "", err
		}

		parts = append(parts, *part)
		partNumber++
		offset = end
	}

	return s.CompleteMultipartUpload(ctx, bucket, key, upload.UploadID, parts)
}
