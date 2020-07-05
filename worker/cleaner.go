package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adrianchifor/Bigbucket/store"
	"github.com/adrianchifor/Bigbucket/utils"
	"github.com/adrianchifor/go-parallel"
	"github.com/gin-gonic/gin"
)

var (
	stopCleaner      = false
	stopCleanerMutex = &sync.Mutex{}
)

// Run cleaner once or on an interval
func RunCleaner(interval int) {
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	deleteJobPool := parallel.LargeJobPool()
	defer deleteJobPool.Close()

	go cleanerGracefulShutdown(deleteJobPool, quit, done)

	log.Printf("Running cleaner...")
	cleanupTables(deleteJobPool)
	cleanupColumns(deleteJobPool)

	if interval > 0 {
		log.Printf("Running cleaner every %d seconds...", interval)
		ticker := time.NewTicker(time.Second * time.Duration(interval))
		defer ticker.Stop()
	loop:
		for {
			select {
			case <-ticker.C:
				cleanupTables(deleteJobPool)
				cleanupColumns(deleteJobPool)
			case <-done:
				log.Println("Cleaner schedule has been cancelled")
				break loop
			}
		}
	}

	log.Println("Cleaner process done")
}

// Run cleaner on HTTP POSTs
func RunCleanerHttp(port int) {
	deleteJobPool := parallel.LargeJobPool()
	defer deleteJobPool.Close()

	router := gin.Default()

	router.POST("/", func(c *gin.Context) {
		log.Printf("Running cleaner...")
		cleanupTables(deleteJobPool)
		cleanupColumns(deleteJobPool)
		c.String(200, "OK")
	})
	router.GET("/health", func(c *gin.Context) {
		c.String(200, "UP")
	})

	utils.RunServer(port, router)
}

func cleanerGracefulShutdown(jobPool *parallel.JobPool, quit <-chan os.Signal, done chan<- bool) {
	<-quit
	log.Println("Cleaner process is shutting down...")

	stopCleanerMutex.Lock()
	stopCleaner = true
	stopCleanerMutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := jobPool.WaitContext(ctx)
	if err != nil {
		log.Fatalf("Could not gracefully shutdown the cleaner process: %v\n", err)
	}
	close(done)
}

func cleanupTables(jobPool *parallel.JobPool) {
	tablesToDelete := utils.GetState("bigbucket/.delete_tables")
	if len(tablesToDelete) == 0 {
		return
	}

	for i, table := range tablesToDelete {
		objects, err := store.ListObjects(fmt.Sprintf("bigbucket/%s/", table), "", 0)
		if err != nil {
			log.Printf("Failed to list objects in table '%s': %v", table, err)
			continue
		}
		if len(objects) == 0 {
			err := utils.WriteState("bigbucket/.delete_tables", utils.RemoveIndex(tablesToDelete, i))
			if err != nil {
				log.Printf("Failed to update .delete_tables state: %v", err)
			} else {
				log.Printf("Table '%s' cleaned up", table)
			}
			continue
		}

		for _, object := range objects {
			object := object
			jobPool.AddJob(func() {
				stopCleanerMutex.Lock()
				if stopCleaner {
					stopCleanerMutex.Unlock()
					return
				}
				stopCleanerMutex.Unlock()

				store.DeleteObject(object)
			})
		}
	}

	jobPool.Wait()
	// Double check objects and update deleted tables state if nothing left
	cleanupTables(jobPool)
}

func cleanupColumns(jobPool *parallel.JobPool) {
	objects, err := store.ListObjects("bigbucket/", "/", 0)
	if err != nil {
		log.Printf("Failed to list tables: %v", err)
	}
	if len(objects) == 0 {
		return
	}
	tables := utils.CleanupTables(objects)

	noColumnsToDelete := true
	for _, table := range tables {
		columnsToDelete := utils.GetState(fmt.Sprintf("bigbucket/%s/.delete_columns", table))
		if len(columnsToDelete) == 0 {
			continue
		}
		if noColumnsToDelete {
			noColumnsToDelete = false
		}

		objects, err = store.ListObjects(fmt.Sprintf("bigbucket/%s/", table), "", 0)
		if err != nil {
			log.Printf("Failed to list objects in table '%s': %v", table, err)
			continue
		}

		for i, column := range columnsToDelete {
			column := column
			noColumnsFound := true

			for _, object := range objects {
				object := object
				if strings.HasSuffix(object, column) {
					if noColumnsFound {
						noColumnsFound = false
					}

					jobPool.AddJob(func() {
						stopCleanerMutex.Lock()
						if stopCleaner {
							stopCleanerMutex.Unlock()
							return
						}
						stopCleanerMutex.Unlock()

						store.DeleteObject(object)
					})
				}
			}

			jobPool.Wait()

			if noColumnsFound {
				err := utils.WriteState(fmt.Sprintf("bigbucket/%s/.delete_columns", table), utils.RemoveIndex(columnsToDelete, i))
				if err != nil {
					log.Printf("Failed to update %s/.delete_columns state: %v", table, err)
				} else {
					log.Printf("Column '%s' in table '%s' cleaned up", column, table)
				}
			}
		}
	}

	if noColumnsToDelete {
		return
	}
	// Double check objects and update deleted columns state if nothing left
	cleanupColumns(jobPool)
}
