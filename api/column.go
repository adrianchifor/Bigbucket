package api

import (
	"fmt"
	"log"
	"strings"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

// HTTP handlers

func listColumns(c *gin.Context) {
	tableName := c.Query("table")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "Please provide 'table' as a querystring parameter"})
		return
	}
	if !isObjectNameValid(tableName) {
		c.JSON(400, gin.H{"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars)})
		return
	}

	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{"error": "Internal server error, check logs"})
		return
	}
	if search(tables, tableName) == -1 {
		c.JSON(404, gin.H{"error": fmt.Sprintf("'%s' table not found or marked for deletion", tableName)})
		return
	}

	columns, _, err := getColumns(tableName)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{"error": "Internal server error, check logs"})
		return
	}

	c.JSON(200, gin.H{"table": tableName, "columns": columns})
}

func deleteColumn(c *gin.Context) {
	tableName := c.Query("table")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "Please provide 'table' as a querystring parameter"})
		return
	}
	columnName := c.Query("column")
	if columnName == "" {
		c.JSON(400, gin.H{"error": "Please provide 'column' as a querystring parameter"})
		return
	}
	if !isObjectNameValid(tableName) || !isObjectNameValid(columnName) {
		c.JSON(400, gin.H{"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars)})
		return
	}

	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{"error": "Internal server error, check logs"})
		return
	}
	if search(tables, tableName) == -1 {
		c.JSON(404, gin.H{"error": fmt.Sprintf("'%s' table not found or marked for deletion", tableName)})
		return
	}

	columns, columnsToDelete, err := getColumns(tableName)
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{"error": "Internal server error, check logs"})
		return
	}

	if search(columns, columnName) == -1 {
		c.JSON(404, gin.H{"error": fmt.Sprintf("'%s' column not found or marked for deletion in '%s' table", columnName, tableName)})
	} else {
		columnsToDelete = append(columnsToDelete, columnName)
		err = writeState(fmt.Sprintf("bigbucket/%s/.delete_columns", tableName), columnsToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "Internal server error, check logs"})
			return
		}
		c.JSON(200, gin.H{"success": fmt.Sprintf("'%s' column marked for deletion in '%s' table", columnName, tableName)})
	}
}

// Column helper functions

func getColumns(table string) (columns []string, columnsToDelete []string, err error) {
	columns = []string{}
	objects, err := store.ListObjects(fmt.Sprintf("bigbucket/%s/", table), "", 2)
	if err != nil {
		return nil, nil, err
	}
	if len(objects) < 2 {
		return columns, nil, nil
	}

	firstKeyPath := objects[1]
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

	return columns, columnsToDelete, nil
}
