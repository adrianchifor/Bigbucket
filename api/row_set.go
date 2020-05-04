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

func setRow(c *gin.Context) {
	params, err := parseRequiredRequestParams(c, "table", "key")
	if err != nil {
		return
	}

	var jsonPayload map[string]string
	if err := c.BindJSON(&jsonPayload); err != nil {
		c.JSON(400, gin.H{
			"error": "Could not parse JSON payload, needs to follow { column string: value string }",
		})
		return
	}
	if len(jsonPayload) == 0 {
		c.JSON(400, gin.H{
			"error": "Nothing to set, JSON payload is empty. Needs to follow { column string: value string }",
		})
		return
	}

	cleanedJsonPayload := make(map[string]string)
	for column, value := range jsonPayload {
		column := strings.TrimSpace(column)
		if column == "" {
			c.JSON(400, gin.H{
				"error": "Columns cannot be empty",
			})
			return
		}
		if !isObjectNameValid(column) {
			c.JSON(400, gin.H{
				"error": fmt.Sprintf("Columns cannot start with '.' nor contain the following characters: %s", invalidChars),
			})
			return
		}

		cleanedJsonPayload[column] = value
	}

	columnsJobPool := parallel.CustomJobPool(parallel.JobPoolConfig{
		WorkerCount:  len(cleanedJsonPayload),
		JobQueueSize: len(cleanedJsonPayload) * 10,
	})
	defer columnsJobPool.Close()

	writesFailed := map[string]error{}
	writesFailedMutex := &sync.Mutex{}

	for column, value := range cleanedJsonPayload {
		column := column
		value := value
		columnsJobPool.AddJob(func() {
			err := store.WriteObject(fmt.Sprintf("bigbucket/%s/%s/%s", params["table"], params["key"], column), []byte(value))
			if err != nil {
				writesFailedMutex.Lock()
				defer writesFailedMutex.Unlock()

				writesFailed[column] = err
			}
		})
	}

	err = columnsJobPool.Wait()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}
	if len(writesFailed) > 0 {
		columnsFailed := []string{}
		bucketRateLimit := false
		for column, writeErr := range writesFailed {
			log.Print(writeErr)
			if !bucketRateLimit && strings.Contains(writeErr.Error(), "429") {
				bucketRateLimit = true
			}
			columnsFailed = append(columnsFailed, column)
		}

		errorMsg := fmt.Sprintf("Check server logs, some columns failed to persist: %s", columnsFailed)
		if bucketRateLimit {
			errorMsg = fmt.Sprintf("Bucket is rate limiting, some columns failed to persist: %s", columnsFailed)
		}
		c.JSON(500, gin.H{
			"error": errorMsg,
		})
		return
	}

	c.JSON(200, gin.H{
		"success": fmt.Sprintf("Set row key '%s' in table '%s'", params["key"], params["table"]),
	})
}
