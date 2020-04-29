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

	results := make(map[string][]map[string]string)

	if rowKey != "" {

		if len(columnsList) == 0 {
			// TODO get columns
		}

		var err error
		results[rowKey], err = getRowColumns(tableName, rowKey, columnsList)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal error, check server logs",
			})
			return
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

func getRowColumns(table string, rowKey string, columns []string) ([]map[string]string, error) {
	results := make([]map[string]string, 0)
	resultsMutex := &sync.Mutex{}

	columnsJobPool := parallel.SmallJobPool()
	defer columnsJobPool.Close()

	for _, column := range columns {
		// Re-def for goroutine access
		column := column
		columnsJobPool.AddJob(func() {
			columnValue, _ := store.ReadObject(fmt.Sprintf("bigbucket/%s/%s/%s", table, rowKey, column))
			// TODO handle errors
			resultsMutex.Lock()
			defer resultsMutex.Unlock()

			results = append(results, map[string]string{column: string(columnValue)})
		})
	}

	err := columnsJobPool.Wait()
	if err != nil {
		return nil, err
	}

	return results, nil
}
