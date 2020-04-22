package main

import (
	"flag"
	"fmt"
	"os"

	"bigbucket/api"
	"bigbucket/storage"
)

var port int

func init() {
	flag.StringVar(&storage.Project, "project", "", "GCP Project")
	flag.StringVar(&storage.Bucket, "bucket", "", "GCS Bucket")
	flag.IntVar(&port, "port", 8080, "Server port")
	flag.Parse()
}

func main() {
	if storage.Project == "" || storage.Bucket == "" {
		fmt.Println("--project and --bucket flags are required")
		os.Exit(1)
	}

	api.RunServer(port)
}
