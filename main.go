package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"bigbucket/api"
	"bigbucket/store"
)

var port int
var versionFlag bool

const version string = "0.1.0"

func init() {
	flag.StringVar(&store.BucketName, "bucket", "", "Bucket name (required, e.g. gs://<bucket-name>)")
	flag.IntVar(&port, "port", 8080, "Server port")
	flag.BoolVar(&versionFlag, "version", false, "Version")
	flag.Parse()
}

func main() {
	if versionFlag {
		fmt.Println("Bigbucket version", version)
		os.Exit(0)
	}

	if store.BucketName == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if strings.HasPrefix(store.BucketName, "gs://") {
		store.BucketName = strings.Replace(store.BucketName, "gs://", "", 1)
		store.InitGoog()
	} else if strings.HasPrefix(store.BucketName, "s3://") {
		// TODO: Implement S3 backend
		fmt.Println("S3 backend is not yet implemented")
		os.Exit(1)
	} else {
		fmt.Println("--bucket flag supports Google Cloud Storage as 'gs://<bucket-name>'")
		os.Exit(1)
	}

	api.RunServer(port)
}
