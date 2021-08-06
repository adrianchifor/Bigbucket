package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/adrianchifor/Bigbucket/api"
	"github.com/adrianchifor/Bigbucket/store"
	"github.com/adrianchifor/Bigbucket/worker"
)

const version string = "0.2.8"

var (
	port            int
	cleanerFlag     bool
	cleanerInterval int
	cleanerHttpFlag bool
	versionFlag     bool
)

func init() {
	flag.StringVar(&store.BucketName, "bucket", "", "Bucket name (required, e.g. gs://<bucket-name>)")
	flag.IntVar(&port, "port", 0, "Server port (default 8080)")
	flag.BoolVar(&cleanerFlag, "cleaner", false, "Run Bigbucket in cleaner mode (default false). "+
		"Will garbage collect tables and columns marked for deletion. Executes based on --cleaner-interval")
	flag.IntVar(&cleanerInterval, "cleaner-interval", 0, "Bigbucket cleaner interval (default 0, runs only once). "+
		"To run cleaner every hour, you can set --cleaner-interval 3600")
	flag.BoolVar(&cleanerHttpFlag, "cleaner-http", false, "Run Bigbucket in cleaner HTTP mode (default false). "+
		"Executes on HTTP POST to /; to be used with https://cloud.google.com/scheduler/docs/creating")
	flag.BoolVar(&versionFlag, "version", false, "Version")
	flag.Parse()
}

func main() {
	if versionFlag {
		fmt.Println("Bigbucket version", version)
		os.Exit(0)
	}
	if cleanerFlag && cleanerHttpFlag {
		fmt.Println("Specify only one of --cleaner or --cleaner-http. To run immediately, use --cleaner. " +
			"For Cloud Scheduler, use --cleaner-http")
		os.Exit(1)
	}

	parseEnvVars()
	initBucket()

	if cleanerFlag {
		worker.RunCleaner(cleanerInterval)
		os.Exit(0)
	}
	if cleanerHttpFlag {
		worker.RunCleanerHttp(port)
		os.Exit(0)
	}

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

	if !cleanerFlag {
		if _, ok := os.LookupEnv("CLEANER"); ok {
			cleanerFlag = true
		}
	}

	if cleanerInterval == 0 {
		if value, ok := os.LookupEnv("CLEANER_INTERVAL"); ok {
			valueInt, err := strconv.Atoi(value)
			if err != nil {
				fmt.Println("'CLEANER_INTERVAL' environment variable cannot be cast to integer")
				os.Exit(1)
			}
			cleanerInterval = valueInt
		}
	}

	if !cleanerHttpFlag && !cleanerFlag {
		if _, ok := os.LookupEnv("CLEANER_HTTP"); ok {
			cleanerHttpFlag = true
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
