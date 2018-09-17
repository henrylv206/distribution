package jss

import (
	"strings"
	"testing"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

const (
	accessKey = "9tSA2gxR6f607aca"
	secretKey = "Avbv0hCRL9WQRFWPO30oUpsxug9Bn6uIMtzOkZ1t"
	bucket    = "registry.jd.com"
	endpoint  = "http://storage.jd.com"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var jssDriverConstructor func() (storagedriver.StorageDriver, error)
var skipCheck func() (reason string)

func init() {
	multiDel()
	jssDriverConstructor = func() (storagedriver.StorageDriver, error) {
		parameters := DriverParameters{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Bucket:    bucket,
			Endpoint:  endpoint,
			Chunksize: 2 * minChunkSize,
		}
		return New(parameters)
	}

	skipCheck = func() string {
		if accessKey == "" || secretKey == "" || bucket == "" {
			return "Must set JDYUN_ACCESS_KEY_ID, JDYUN_ACCESS_KEY_SECRET, JSS_BUCKET to run OSS tests"
		}
		return ""
	}

	testsuites.RegisterSuite(jssDriverConstructor, skipCheck)
}

func multiDel() {
	bk := &Bucket{
		Client: NewJSSClient(accessKey, secretKey, endpoint, time.Minute),
		Name:   strings.ToLower("registry.jd.com"),
	}

	listResponse, err := bk.List("", "", "", listMax)
	if err != nil {
		return
	}
	jssObjects := make([]Object, listMax)

	for len(listResponse.Contents) > 0 {
		for index, key := range listResponse.Contents {
			jssObjects[index].Key = key.Key
		}

		err := bk.DelMulti(Delete{Quiet: false, Objects: jssObjects[0:len(listResponse.Contents)]})
		if err != nil {
			return
		}

		listResponse, err = bk.List("", "", "", listMax)
		if err != nil {
			return
		}
	}

	multis, _, err := bk.ListMulti("", "")
	if err != nil {
		return
	}
	for _, multi := range multis {
		multi.Abort()
	}
}
