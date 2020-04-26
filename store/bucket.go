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

	Ctx             context.Context
	GoogStoreClient storage.Client
	GoogStoreBucket storage.BucketHandle
)

func InitGoog() {
	Ctx = context.Background()
	GoogStoreClient, err := storage.NewClient(Ctx)
	if err != nil {
		log.Fatalf("Failed to create Google Storage client: %v", err)
	}

	GoogStoreBucket = *GoogStoreClient.Bucket(BucketName)
}

func ListObjects(prefix string, delimiter string) ([]string, error) {
	ctxTimeout, cancel := context.WithTimeout(Ctx, time.Second*30)
	defer cancel()

	query := &storage.Query{Prefix: prefix, Delimiter: delimiter}

	var names []string
	it := GoogStoreBucket.Objects(ctxTimeout, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, attrs.Name)
	}

	return names, nil
}

func WriteObject(object string, data []byte) error {
	if len(object) == 0 {
		return errors.New("store.WriteObject: object cannot be empty string")
	}
	if data == nil {
		return errors.New("store.WriteObject: data cannot be nil")
	}

	ctxTimeout, cancel := context.WithTimeout(Ctx, time.Second*30)
	defer cancel()

	obj := GoogStoreBucket.Object(object)
	w := obj.NewWriter(ctxTimeout)

	compressedData, err := zstd.Compress(nil, data)
	if err != nil {
		return err
	}
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

	ctxTimeout, cancel := context.WithTimeout(Ctx, time.Second*30)
	defer cancel()

	obj := GoogStoreBucket.Object(object)
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

	ctxTimeout, cancel := context.WithTimeout(Ctx, time.Second*10)
	defer cancel()

	obj := GoogStoreBucket.Object(object)
	if err := obj.Delete(ctxTimeout); err != nil {
		return err
	}

	return nil
}
