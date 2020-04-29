package api

import (
	"fmt"
	"log"
	// "strconv"
	"strings"
	"sync"

	"bigbucket/store"
	"github.com/adrianchifor/go-parallel"
	"github.com/gin-gonic/gin"
)

func getRows(c *gin.Context) {
	tableName := c.Query("table")
	if tableName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'table' as a querystring parameter",
		})
		return
	}
	rowKey := c.Query("key")
	rowPrefix := c.Query("prefix")
	if rowKey != "" && rowPrefix != "" {
		c.JSON(400, gin.H{
			"error": "Please provide only one of 'key' or 'prefix' as a querystring parameter",
		})
		return
	}
	columns := c.Query("columns")
	rowsCount := c.Query("count")

	if !isObjectNameValid(tableName) || !isObjectNameValid(rowKey) || !isObjectNameValid(rowPrefix) ||
		!isObjectNameValid(columns) || !isObjectNameValid(rowsCount) {

		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
		return
	}

	columnsList := []string{}
	if columns != "" {
		columnsList = strings.Split(strings.TrimSpace(columns), ",")
	}

	results := make(map[string][]map[string]string)
	resultsLock := &sync.Mutex{}

	if rowKey != "" {
		results[rowKey] = make([]map[string]string, 0)

		if len(columnsList) > 0 {
			// We should be able to fetch ~10 columns in parallel
			columnsJobPool := parallel.SmallJobPool()
			defer columnsJobPool.Close()

			for _, column := range columnsList {
				// Re-def for access in goroutines below
				column := column
				columnsJobPool.AddJob(func() {
					columnValue, _ := store.ReadObject(fmt.Sprintf("bigbucket/%s/%s/%s", tableName, rowKey, column))

					resultsLock.Lock()
					defer resultsLock.Unlock()

					results[rowKey] = append(results[rowKey], map[string]string{column: string(columnValue)})
				})
			}

			err := columnsJobPool.Wait()
			if err != nil {
				log.Print(err)
				c.JSON(500, gin.H{
					"error": "Internal server error, check logs",
				})
				return
			}
		} else {

		}
	} else {
		// rowsCountInt := 0
		// if rowsCount != "" {
		// 	if n, err := strconv.Atoi(rowsCount); err == nil {
		// 		rowsCountInt = n
		// 	} else {
		// 		c.JSON(400, gin.H{"error": "'count' parameter has to be an integer"})
		// 		return
		// 	}
		// }
	}

	c.JSON(200, results)
}

func setRow(c *gin.Context) {
	tableName := c.Query("table")
	if tableName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'table' as a querystring parameter",
		})
		return
	}
	rowKey := c.Query("key")
	if rowKey == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'key' as a querystring parameter",
		})
		return
	}

	if !isObjectNameValid(tableName) || !isObjectNameValid(rowKey) {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
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

	columnsJobPool := parallel.SmallJobPool()
	defer columnsJobPool.Close()

	writesFailed := map[string]error{}
	writesFailedLock := &sync.Mutex{}

	for column, value := range jsonPayload {
		if column == "" {
			continue
		}
		// Re-def for access in goroutines below
		column := column
		value := value

		columnsJobPool.AddJob(func() {
			err := store.WriteObject(fmt.Sprintf("bigbucket/%s/%s/%s", tableName, rowKey, column), []byte(value))
			if err != nil {
				writesFailedLock.Lock()
				defer writesFailedLock.Unlock()

				writesFailed[column] = err
			}
		})
	}

	err := columnsJobPool.Wait()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal server error, check logs",
		})
		return
	}
	if len(writesFailed) > 0 {
		columnsFailed := []string{}
		for column, writeErr := range writesFailed {
			log.Print(writeErr)
			columnsFailed = append(columnsFailed, column)
		}
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("Check logs, some columns failed to persist: %s", columnsFailed),
		})
		return
	}

	c.JSON(200, gin.H{
		"success": fmt.Sprintf("Set row key '%s' in table '%s'", rowKey, tableName),
	})
}

func deleteRow(c *gin.Context) {
	c.JSON(200, "")
}
