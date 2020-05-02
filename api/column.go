package api

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

func listColumns(c *gin.Context) {
	tableName := strings.TrimSpace(c.Query("table"))
	if tableName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'table' as a querystring parameter",
		})
		return
	}
	if !isObjectNameValid(tableName) {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
		return
	}

	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}
	if search(tables, tableName) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", tableName),
		})
		return
	}

	columns, _, err := getColumns(tableName)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	c.JSON(200, gin.H{"table": tableName, "columns": columns})
}

func deleteColumn(c *gin.Context) {
	tableName := strings.TrimSpace(c.Query("table"))
	if tableName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'table' as a querystring parameter",
		})
		return
	}
	columnName := strings.TrimSpace(c.Query("column"))
	if columnName == "" {
		c.JSON(400, gin.H{
			"error": "Please provide 'column' as a querystring parameter",
		})
		return
	}
	if !isObjectNameValid(tableName) || !isObjectNameValid(columnName) {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
		return
	}

	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}
	if search(tables, tableName) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", tableName),
		})
		return
	}

	columns, columnsToDelete, err := getColumns(tableName)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	if search(columns, columnName) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Column '%s' not found or marked for deletion in table '%s'", columnName, tableName),
		})
	} else {
		columnsToDelete = append(columnsToDelete, columnName)
		err = writeState(fmt.Sprintf("bigbucket/%s/.delete_columns", tableName), columnsToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal error, check server logs",
			})
			return
		}
		c.JSON(200, gin.H{
			"success": fmt.Sprintf("Column '%s' marked for deletion in table '%s'", columnName, tableName),
		})
	}
}

func getColumns(table string) (columns []string, columnsToDelete []string, err error) {
	columns = []string{}
	objects, err := store.ListObjects(fmt.Sprintf("bigbucket/%s/", table), "", 2)
	if err != nil {
		return nil, nil, err
	}
	if len(objects) < 2 {
		return columns, nil, nil
	}
	indexDelete := search(objects, fmt.Sprintf("bigbucket/%s/.delete_columns", table))
	if indexDelete > -1 {
		objects = removeIndex(objects, indexDelete)
	}

	firstKey := strings.Split(objects[0], "/")[2]
	firstKeyPath := fmt.Sprintf("bigbucket/%s/%s/", table, firstKey)
	objects, err = store.ListObjects(firstKeyPath, "", 0)
	if err != nil {
		return nil, nil, err
	}

	for _, column := range objects {
		cleanColumn := strings.Replace(column, firstKeyPath, "", 1)
		if cleanColumn != "" {
			columns = append(columns, cleanColumn)
		}
	}

	// Remove columns marked for deletion from results
	columnsToDelete = getState(fmt.Sprintf("bigbucket/%s/.delete_columns", table))
	for _, columnToDelete := range columnsToDelete {
		index := search(columns, columnToDelete)
		if index > -1 {
			columns = removeIndex(columns, index)
		}
	}

	sort.Strings(columns)
	return columns, columnsToDelete, nil
}
