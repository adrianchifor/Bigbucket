package api

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"bigbucket/store"
	"github.com/adrianchifor/go-parallel"
	"github.com/gin-gonic/gin"
)

func deleteRows(c *gin.Context) {
	params, err := parseRequiredRequestParams(c, "table")
	if err != nil {
		return
	}
	rowKey, rowPrefix, err := parseExclusiveRequestParams(c, "key", "prefix")
	if err != nil {
		return
	}
	if rowKey == "" && rowPrefix == "" {
		c.JSON(400, gin.H{
			"error": "Please provide one of 'key' or 'prefix' as a querystring parameter. To delete the table use /api/table",
		})
		return
	}

	keyPath := fmt.Sprintf("bigbucket/%s/%s/", params["table"], rowKey)
	if rowPrefix != "" {
		keyPath = fmt.Sprintf("bigbucket/%s/%s", params["table"], rowPrefix)
	}

	objects, err := store.ListObjects(keyPath, "", 0)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}
	if len(objects) == 0 {
		errMsg := fmt.Sprintf("Row key '%s' not found in table '%s'", rowKey, params["table"])
		if rowPrefix != "" {
			errMsg = fmt.Sprintf("Rows with key prefix '%s' not found in table '%s'", rowPrefix, params["table"])
		}
		c.JSON(404, gin.H{
			"error": errMsg,
		})
		return
	}

	deleteJobPool := parallel.CustomJobPool(parallel.JobPoolConfig{
		WorkerCount:  len(objects),
		JobQueueSize: len(objects) * 10,
	})
	defer deleteJobPool.Close()

	deletesFailed := map[string]error{}
	deletesFailedMutex := &sync.Mutex{}

	for _, object := range objects {
		object := object
		deleteJobPool.AddJob(func() {
			err := store.DeleteObject(object)
			if err != nil {
				objectSplit := strings.Split(object, "/")
				failedKey := objectSplit[2]
				failedColumn := objectSplit[3]

				deletesFailedMutex.Lock()
				defer deletesFailedMutex.Unlock()

				deletesFailed[fmt.Sprintf("%s/%s", failedKey, failedColumn)] = err
			}
		})
	}

	err = deleteJobPool.Wait()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}
	if len(deletesFailed) > 0 {
		keyColumnsFailed := []string{}
		bucketRateLimit := false
		for keyColumn, deleteErr := range deletesFailed {
			log.Print(deleteErr)
			if !bucketRateLimit && strings.Contains(deleteErr.Error(), "429") {
				bucketRateLimit = true
			}
			keyColumnsFailed = append(keyColumnsFailed, keyColumn)
		}

		errorMsg := fmt.Sprintf("Check server logs, some columns failed to be deleted: %s", keyColumnsFailed)
		if bucketRateLimit {
			errorMsg = fmt.Sprintf("Bucket is rate limiting, some columns failed to be deleted: %s", keyColumnsFailed)
		}
		c.JSON(500, gin.H{
			"error": errorMsg,
		})
		return
	}

	successMsg := fmt.Sprintf("Row with key '%s' was deleted from table '%s'", rowKey, params["table"])
	if rowPrefix != "" {
		successMsg = fmt.Sprintf("Rows with key prefix '%s' were deleted from table '%s'", rowPrefix, params["table"])
	}
	c.JSON(200, gin.H{
		"success": successMsg,
	})
}
