package main

import (
	"flag"
	"fmt"
	"os"

	"bigbucket/api"
	"bigbucket/storage"
)

var port int
var versionFlag bool
var version string = "0.1"

func init() {
	flag.StringVar(&storage.Project, "project", "", "GCP Project (required)")
	flag.StringVar(&storage.Bucket, "bucket", "", "GCS Bucket (required)")
	flag.IntVar(&port, "port", 8080, "Server port")
	flag.BoolVar(&versionFlag, "version", false, "Version")
	flag.Parse()
}

func main() {
	if versionFlag {
		fmt.Println("Bigbucket version", version)
		os.Exit(0)
	}

	if storage.Project == "" || storage.Bucket == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	api.RunServer(port)
}
