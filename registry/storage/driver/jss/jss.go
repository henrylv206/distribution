package jss

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	log "github.com/Sirupsen/logrus"
)

const (
	driverName = "jss"

	minChunkSize     = 5 << 20
	defaultChunkSize = 2 * minChunkSize
	listMax          = 1000

	timeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
)

type DriverParameters struct {
	AccessKey string
	SecretKey string
	Bucket    string
	Endpoint  string
	Chunksize int64
}

func init() {
	factory.Register(driverName, &jssDriverFactory{})
}

// jssDriverFactory implements the factory.StorageDriverFactory interface
type jssDriverFactory struct{}

func (factory *jssDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Bucket    *Bucket
	ChunkSize int64
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by JSS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	accessKey, ok := parameters["accesskey"]
	if !ok {
		return nil, fmt.Errorf("No accesskey parameter provided")
	}

	secretKey, ok := parameters["secretkey"]
	if !ok {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		return nil, fmt.Errorf("No endpoint parameter provided")
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid valud for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}
	}

	params := DriverParameters{
		AccessKey: fmt.Sprint(accessKey),
		SecretKey: fmt.Sprint(secretKey),
		Bucket:    fmt.Sprint(bucket),
		Endpoint:  fmt.Sprint(endpoint),
		Chunksize: chunkSize,
	}
	return New(params)
}

func New(params DriverParameters) (*Driver, error) {
	client := NewJSSClient(params.AccessKey, params.SecretKey, params.Endpoint, 50*time.Minute)

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					Bucket: &Bucket{
						Client: client,
						Name:   strings.ToLower(params.Bucket),
					},
					ChunkSize: params.Chunksize,
				},
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	content, err := d.Bucket.Get(pathToKey(path), 0)
	if err != nil {
		return nil, parseError(path, err)
	}
	return content, nil
}

func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// writer, err := d.Writer(ctx, path, false)
	// if err != nil {
	// 	return err
	// }
	// defer writer.Close()
	// _, err = io.Copy(writer, bytes.NewReader(contents))
	// if err != nil {
	// 	writer.Cancel()
	// 	return err
	// }
	// return writer.Commit()
	return parseError(path, d.Bucket.Put(pathToKey(path), contents))
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	data, err := d.Bucket.Get(pathToKey(path), offset)
	if err != nil {
		return nil, parseError(path, err)
	}
	return ioutil.NopCloser(bytes.NewBuffer(data)), nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	key := pathToKey(path)
	if !append {
		multi, err := d.Bucket.NewMulti(key)
		if err != nil {
			return nil, err
		}
		return d.newWriter(key, multi, nil), nil
	}
	multis, _, err := d.Bucket.ListMulti(key, "")
	if err != nil {
		return nil, parseError(path, err)
	}
	for _, multi := range multis {
		if key != multi.Key {
			continue
		}
		parts, err := multi.ListParts()
		if err != nil {
			return nil, parseError(path, err)
		}
		return d.newWriter(key, multi, parts), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	listResponse, err := d.Bucket.List(pathToKey(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(listResponse.Contents) == 1 {
		if listResponse.Contents[0].Key != pathToKey(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = listResponse.Contents[0].Size

			timestamp, err := time.Parse(timeFormat, listResponse.Contents[0].LastModified)
			if err != nil {
				return nil, storagedriver.InvalidPathError{Path: path}
			}
			fi.ModTime = timestamp
		}
	} else if len(listResponse.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}
	path = pathToKey(path)

	listResponse, err := d.Bucket.List(path, "/", "", listMax)
	if err != nil {
		return nil, parseError(opath, err)
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listResponse.Contents {
			files = append(files, keyToPath(key.Key))
		}

		for _, commonPrefix := range listResponse.CommonPrefixes {
			directories = append(directories, keyToPath(commonPrefix))
		}

		if listResponse.HasNext {
			listResponse, err = d.Bucket.List(path, "/", listResponse.NextMarker, listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	err := d.Bucket.CopyObj(pathToKey(sourcePath), pathToKey(destPath), false)
	if err != nil {
		return parseError(sourcePath, err)
	}
	return d.Delete(ctx, sourcePath)
}

func (d *driver) Delete(ctx context.Context, path string) error {
	listResponse, err := d.Bucket.List(pathToKey(path), "", "", listMax)
	if err != nil || len(listResponse.Contents) == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}

	jssObjects := make([]Object, listMax)

	for len(listResponse.Contents) > 0 {
		for index, key := range listResponse.Contents {
			jssObjects[index].Key = key.Key
		}

		err := d.Bucket.DelMulti(Delete{Quiet: false, Objects: jssObjects[0:len(listResponse.Contents)]})
		if err != nil {
			return nil
		}

		listResponse, err = d.Bucket.List(pathToKey(path), "", "", listMax)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(20 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}

	log.Infof("9. debug jss, jss.go urlfor, SignedURLWithMethod, bucket name: %s", d.Bucket.Name)

	return d.Bucket.SignedURLWithMethod(methodString, pathToKey(path), expiresTime, nil, nil), nil
}

type writer struct {
	driver      *driver
	key         string
	multi       *Multi
	parts       []Part
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

func (d *driver) newWriter(key string, multi *Multi, parts []Part) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += part.Size
	}
	return &writer{
		driver: d,
		key:    key,
		multi:  multi,
		parts:  parts,
		size:   size,
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload :sadface:
	// if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
	// 	return 0, fmt.Errorf("++++++++++++++++________________++++++++++++++++")
	// }
	// if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
	// 	err := w.multi.Complete(w.parts)
	// 	if err != nil {
	// 		w.multi.Abort()
	// 		return 0, err
	// 	}

	// 	multi, err := w.driver.Bucket.NewMulti(w.key)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	w.multi = multi

	// 	// If the entire written file is smaller than minChunkSize, we need to make
	// 	// a new part from scratch :double sad face:
	// 	if w.size < minChunkSize {
	// 		contents, err := w.driver.Bucket.Get(w.key, 0)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 		w.parts = nil
	// 		w.readyPart = contents
	// 	} else {
	// 		// Otherwise we can use the old file as the new first part
	// 		// err := multi.Bucket.CopyObj()//.PutPartCopy(1, s3.CopyOptions{}, w.driver.Bucket.Name+"/"+w.key)
	// 		// if err != nil {
	// 		// 	return 0, err
	// 		// }
	// 		// w.parts = []Part{part}
	// 		return 0, fmt.Errorf("++++++++++++++++________________++++++++++++++++")
	// 	}
	// }

	var n int

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := int(w.driver.ChunkSize) - len(w.readyPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
			} else {
				w.readyPart = append(w.readyPart, p...)
				n += len(p)
				p = nil
			}
		}

		if neededBytes := int(w.driver.ChunkSize) - len(w.pendingPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart()
				if err != nil {
					w.size += int64(n)
					return n, err
				}
			} else {
				w.pendingPart = append(w.pendingPart, p...)
				n += len(p)
				p = nil
			}
		}
	}
	w.size += int64(n)
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	err := w.multi.Abort()
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true
	err = w.multi.Complete(w.parts)
	if err != nil {
		w.multi.Abort()
		return err
	}
	return nil
}

func (w *writer) flushPart() error {
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}
	if len(w.pendingPart) < int(w.driver.ChunkSize) {
		// closing with a small pending part
		// combine ready and pending to avoid writing a small part
		w.readyPart = append(w.readyPart, w.pendingPart...)
		w.pendingPart = nil
	}

	part, err := w.multi.PutPart(len(w.parts)+1, w.readyPart)
	if err != nil {
		return err
	}
	w.parts = append(w.parts, part)
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	return nil
}

func parseError(path string, err error) error {
	if jssErr, ok := err.(*Error); ok && jssErr.StatusCode == http.StatusNotFound && (jssErr.Code == "NoSuchKey" || jssErr.Code == "") {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return err
}

func pathToKey(path string) string {
	key := strings.TrimPrefix(path, "/")
	key = strings.TrimPrefix(key, "docker/registry/v2/")
	key = strings.Replace(key, "repositories/", "rep/", 1)
	key = strings.Replace(key, "/_layers/", "/lyr/", 1)
	key = strings.Replace(key, "blobs/", "blb/", 1)
	key = strings.Replace(key, "/sha256/", "/sha/", 1)
	key = strings.Replace(key, "/link", "/lk", 1)

	key = strings.Replace(key, "/_manifests/", "/mft/", 1)
	key = strings.Replace(key, "/revisions/", "/rev/", 1)

	key = strings.Replace(key, "/tags/", "/tg/", 1)
	key = strings.Replace(key, "/latest/", "/lst/", 1)
	key = strings.Replace(key, "/index/", "/idx/", 1)

	return key
	// return strings.TrimPrefix(path, "/")
}

func keyToPath(key string) string {
	path := "/docker/registry/v2/" + strings.TrimSuffix(key, "/")
	path = strings.Replace(path, "rep/", "repositories/", 1)
	path = strings.Replace(path, "/lyr/", "/_layers/", 1)
	path = strings.Replace(path, "blb/", "blobs/", 1)
	path = strings.Replace(path, "/sha/", "/sha256/", 1)
	path = strings.Replace(path, "/lk", "/link", 1)

	path = strings.Replace(path, "/mft/", "/_manifests/", 1)
	path = strings.Replace(path, "/rev/", "/revisions/", 1)

	path = strings.Replace(path, "/tg/", "/tags/", 1)
	path = strings.Replace(path, "/lst/", "/latest/", 1)
	path = strings.Replace(path, "/idx/", "/index/", 1)

	return path
	// return "/" + strings.TrimSuffix(key, "/")
}
