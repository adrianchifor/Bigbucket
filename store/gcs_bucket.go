package store

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/storage"
	"github.com/DataDog/zstd"
	"google.golang.org/api/iterator"
)

var (
	BucketName string
	GoogBucket storage.BucketHandle
)

func InitGoog() {
	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create Google Storage client: %v", err)
	}

	GoogBucket = *gcsClient.Bucket(BucketName)
}

func ListObjects(prefix string, delimiter string, limit int) ([]string, error) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	query := &storage.Query{Prefix: prefix, Delimiter: delimiter}
	it := GoogBucket.Objects(ctxTimeout, query)

	var objects []string
	count := 0
	for {
		if limit > 0 && count == limit {
			return objects, nil
		}
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if delimiter != "" {
			objects = append(objects, attrs.Prefix)
		} else {
			objects = append(objects, attrs.Name)
		}
		count++
	}

	return objects, nil
}

func WriteObject(object string, data []byte) error {
	if len(object) == 0 {
		return errors.New("store.WriteObject: object cannot be empty string")
	}
	if data == nil {
		return errors.New("store.WriteObject: data cannot be nil")
	}

	compressedData, err := zstd.Compress(nil, data)
	if err != nil {
		return err
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	obj := GoogBucket.Object(object)

	w := obj.NewWriter(ctxTimeout)
	w.Write(compressedData)

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

func ReadObject(object string) ([]byte, error) {
	if len(object) == 0 {
		return nil, errors.New("store.ReadObject: object cannot be empty string")
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	obj := GoogBucket.Object(object)

	r, err := obj.NewReader(ctxTimeout)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	compressedData, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	data, err := zstd.Decompress(nil, compressedData)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func DeleteObject(object string) error {
	if len(object) == 0 {
		return errors.New("store.DeleteObject: object cannot be empty string")
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	obj := GoogBucket.Object(object)

	if err := obj.Delete(ctxTimeout); err != nil {
		return err
	}

	return nil
}
