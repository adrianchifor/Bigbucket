package main

import (
	"flag"
	"fmt"
	"os"

	"bigbucket/api"
	"bigbucket/store"
)

var port int
var versionFlag bool

const version string = "0.1.0"

func init() {
	flag.StringVar(&store.BucketName, "bucket", "", "GCS Bucket name (required)")
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

	store.InitGoog()

	api.RunServer(port)
}
