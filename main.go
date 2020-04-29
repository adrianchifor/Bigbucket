package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"bigbucket/api"
	"bigbucket/store"
)

var port int
var versionFlag bool

const version string = "0.1.0"

func init() {
	flag.StringVar(&store.BucketName, "bucket", "", "Bucket name (required, e.g. gs://<bucket-name>)")
	flag.IntVar(&port, "port", 0, "Server port")
	flag.BoolVar(&versionFlag, "version", false, "Version")
	flag.Parse()
}

func main() {
	if versionFlag {
		fmt.Println("Bigbucket version", version)
		os.Exit(0)
	}

	parseEnvVars()
	initBucket()

	api.RunServer(port)
}

func parseEnvVars() {
	if store.BucketName == "" {
		if value, ok := os.LookupEnv("BUCKET"); ok {
			store.BucketName = value
		} else {
			flag.PrintDefaults()
			os.Exit(1)
		}
	}

	if port == 0 {
		if value, ok := os.LookupEnv("PORT"); ok {
			valueInt, err := strconv.Atoi(value)
			if err != nil {
				fmt.Println("'PORT' environment variable cannot be cast to integer")
				os.Exit(1)
			}
			port = valueInt
		} else {
			// Use default is neither --port / PORT are defined
			port = 8080
		}
	}
}

func initBucket() {
	if strings.HasPrefix(store.BucketName, "gs://") {
		store.BucketName = strings.Replace(store.BucketName, "gs://", "", 1)
		store.InitGoog()
	} else if strings.HasPrefix(store.BucketName, "s3://") {
		// TODO: Implement S3 backend
		fmt.Println("S3 bucket backend is not yet implemented")
		os.Exit(1)
	} else {
		fmt.Println("--bucket flag or 'BUCKET' env supports Google Cloud Storage as 'gs://<bucket-name>'")
		os.Exit(1)
	}
}
