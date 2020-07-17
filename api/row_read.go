package api

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/adrianchifor/Bigbucket/store"
	"github.com/adrianchifor/Bigbucket/utils"
	"github.com/adrianchifor/go-parallel"
	"github.com/gin-gonic/gin"
)

func getRows(c *gin.Context) {
	allowCORSForBrowsers(c)
	tableMap, err := parseRequiredRequestParams(c, "table")
	if err != nil {
		return
	}
	rowKey, rowPrefix, err := parseExclusiveRequestParams(c, "key", "prefix")
	if err != nil {
		return
	}
	columnsCountMap, err := parseOptionalRequestParams(c, "columns", "limit")
	if err != nil {
		return
	}
	params := utils.MergeMaps(tableMap, columnsCountMap)

	columnsList := []string{}
	if params["columns"] != "" {
		columnsList = strings.Split(params["columns"], ",")
	}

	results := make(map[string]map[string]string)

	// When a specific key and columns are requested (no queries, direct fetches)
	if rowKey != "" && len(columnsList) > 0 {
		var err error
		results[rowKey], err = getRowColumns(params["table"], rowKey, columnsList)
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

	rowsLimitInt := 0
	if params["limit"] != "" {
		if n, err := strconv.Atoi(params["limit"]); err == nil {
			rowsLimitInt = n
		} else {
			c.JSON(400, gin.H{"error": "'limit' parameter has to be an integer"})
			return
		}
	}

	keyPath := fmt.Sprintf("bigbucket/%s/", params["table"])
	if rowKey != "" {
		keyPath = fmt.Sprintf("bigbucket/%s/%s/", params["table"], rowKey)
	} else if rowPrefix != "" {
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
		errMsg := fmt.Sprintf("Table '%s' not found", params["table"])
		if rowKey != "" {
			errMsg = fmt.Sprintf("Row key '%s' not found in table '%s'", rowKey, params["table"])
		} else if rowPrefix != "" {
			errMsg = fmt.Sprintf("Rows with key prefix '%s' not found in table '%s'", rowPrefix, params["table"])
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
	resultsMutex := &sync.Mutex{}

	sort.Strings(objects)
	for _, object := range objects {
		object := object
		if strings.HasSuffix(object, "/") || strings.Count(object, "/") < 3 {
			// Skip if object is not column
			continue
		}
		objectSplit := strings.Split(object, "/")
		objectKey := objectSplit[2]
		objectColumn := objectSplit[3]
		if len(columnsList) > 0 && utils.Search(columnsList, objectColumn) == -1 {
			// Skip if current column is not in specified columns
			continue
		}

		resultsMutex.Lock()
		if _, exists := results[objectKey]; !exists {
			if rowsLimitInt > 0 {
				if rowsAdded == rowsLimitInt {
					// Break loop if max row limit is reached
					resultsMutex.Unlock()
					break
				}
				rowsAdded++
			}
			results[objectKey] = make(map[string]string)
		}
		resultsMutex.Unlock()

		rowsJobPool.AddJob(func() {
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

func getRowsCount(c *gin.Context) {
	rows, table, err := listRowKeys(c)
	if err != nil {
		return
	}

	c.JSON(200, gin.H{"table": table, "rowsCount": strconv.Itoa(len(rows))})
}

func listRows(c *gin.Context) {
	rows, table, err := listRowKeys(c)
	if err != nil {
		return
	}

	rowKeys := []string{}
	for _, row := range rows {
		rowKey := strings.Split(row, "/")[2]
		rowKeys = append(rowKeys, rowKey)
	}
	sort.Strings(rowKeys)

	c.JSON(200, gin.H{"table": table, "rowKeys": rowKeys})
}

func listRowKeys(c *gin.Context) ([]string, string, error) {
	tableMap, err := parseRequiredRequestParams(c, "table")
	if err != nil {
		return nil, "", err
	}
	prefixMap, err := parseOptionalRequestParams(c, "prefix")
	if err != nil {
		return nil, "", err
	}
	params := utils.MergeMaps(tableMap, prefixMap)

	keysPath := fmt.Sprintf("bigbucket/%s/", params["table"])
	if params["prefix"] != "" {
		keysPath = fmt.Sprintf("bigbucket/%s/%s", params["table"], params["prefix"])
	}

	rows, err := store.ListObjects(keysPath, "/", 0)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return nil, "", err
	}

	return rows, params["table"], nil
}
