package api

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"bigbucket/store"
	"github.com/adrianchifor/go-parallel"
	"github.com/gin-gonic/gin"
)

func getRows(c *gin.Context) {
	tableName := strings.TrimSpace(c.Query("table"))
	if tableName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'table' as a querystring parameter",
		})
		return
	}
	rowKey := strings.TrimSpace(c.Query("key"))
	rowPrefix := strings.TrimSpace(c.Query("prefix"))
	if rowKey != "" && rowPrefix != "" {
		c.JSON(400, gin.H{
			"error": "Please provide only one of 'key' or 'prefix' as a querystring parameter",
		})
		return
	}
	columns := strings.TrimSpace(c.Query("columns"))
	rowsCount := strings.TrimSpace(c.Query("count"))

	if !isObjectNameValid(tableName) || !isObjectNameValid(rowKey) || !isObjectNameValid(rowPrefix) ||
		!isObjectNameValid(columns) || !isObjectNameValid(rowsCount) {

		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
		return
	}

	columnsList := []string{}
	if columns != "" {
		columnsList = strings.Split(columns, ",")
	}

	results := make(map[string]map[string]string)

	// When a specific key and columns are requested (no queries, direct fetches)
	if rowKey != "" && len(columnsList) > 0 {
		var err error
		results[rowKey], err = getRowColumns(tableName, rowKey, columnsList)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal error, check server logs",
			})
			return
		}
		c.JSON(200, results)
		return
	}

	rowsCountInt := 0
	if rowsCount != "" {
		if n, err := strconv.Atoi(rowsCount); err == nil {
			rowsCountInt = n
		} else {
			c.JSON(400, gin.H{"error": "'count' parameter has to be an integer"})
			return
		}
	}

	keyPath := fmt.Sprintf("bigbucket/%s/", tableName)
	if rowKey != "" {
		keyPath = fmt.Sprintf("bigbucket/%s/%s/", tableName, rowKey)
	} else if rowPrefix != "" {
		keyPath = fmt.Sprintf("bigbucket/%s/%s", tableName, rowPrefix)
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
		errMsg := fmt.Sprintf("Table '%s' not found", tableName)
		if rowKey != "" {
			errMsg = fmt.Sprintf("Row key '%s' not found in table '%s'", rowKey, tableName)
		} else if rowPrefix != "" {
			errMsg = fmt.Sprintf("Rows with key prefix '%s' not found in table '%s'", rowPrefix, tableName)
		}
		c.JSON(404, gin.H{
			"error": errMsg,
		})
		return
	}

	rowsJobPool := parallel.CustomJobPool(parallel.JobPoolConfig{
		WorkerCount:  len(objects),
		JobQueueSize: len(objects) * 10,
	})
	defer rowsJobPool.Close()

	rowsAdded := 0
	rowsAddedMutex := &sync.Mutex{}
	resultsMutex := &sync.Mutex{}

	for _, object := range objects {
		object := object
		rowsJobPool.AddJob(func() {
			// End goroutine if object is not column
			if strings.HasSuffix(object, "/") || strings.Count(object, "/") < 3 {
				return
			}

			objectSplit := strings.Split(object, "/")
			objectKey := objectSplit[2]
			objectColumn := objectSplit[3]

			resultsMutex.Lock()
			if _, exists := results[objectKey]; !exists {
				if rowsCountInt > 0 {
					rowsAddedMutex.Lock()
					if rowsAdded == rowsCountInt {
						// End goroutine if max row count is reached
						rowsAddedMutex.Unlock()
						resultsMutex.Unlock()
						return
					}
					rowsAdded++
					rowsAddedMutex.Unlock()
				}
				results[objectKey] = make(map[string]string)
			}
			resultsMutex.Unlock()

			if len(columnsList) > 0 && search(columnsList, objectColumn) == -1 {
				// End goroutine if current column is not in the specified columns
				return
			}

			columnValue, err := store.ReadObject(object)
			if err != nil {
				log.Print(err, fmt.Sprintf(" (%s)", object))
				return
			}
			resultsMutex.Lock()
			defer resultsMutex.Unlock()
			results[objectKey][objectColumn] = string(columnValue)
		})
	}

	err = rowsJobPool.Wait()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	c.JSON(200, results)
}

func getRowColumns(table string, rowKey string, columns []string) (map[string]string, error) {
	results := make(map[string]string)
	resultsMutex := &sync.Mutex{}

	columnsJobPool := parallel.CustomJobPool(parallel.JobPoolConfig{
		WorkerCount:  len(columns),
		JobQueueSize: len(columns) * 10,
	})
	defer columnsJobPool.Close()

	for _, column := range columns {
		column := column
		columnsJobPool.AddJob(func() {
			columnPath := fmt.Sprintf("bigbucket/%s/%s/%s", table, rowKey, column)
			columnValue, err := store.ReadObject(columnPath)
			if err != nil {
				log.Print(err, fmt.Sprintf(" (%s)", columnPath))
				return
			}
			resultsMutex.Lock()
			defer resultsMutex.Unlock()
			results[column] = string(columnValue)
		})
	}

	err := columnsJobPool.Wait()
	if err != nil {
		return nil, err
	}

	return results, nil
}
