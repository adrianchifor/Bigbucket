package api

import (
	"fmt"
	"log"
	"strings"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

// HTTP handlers

func listTables(c *gin.Context) {
	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal server error, check logs",
		})
		return
	}

	c.JSON(200, gin.H{"tables": tables})
}

func deleteTable(c *gin.Context) {
	tableName := c.Query("table")
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

	tables, tablesToDelete, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal server error, check logs",
		})
		return
	}

	if search(tables, tableName) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", tableName),
		})
	} else {
		tablesToDelete = append(tablesToDelete, tableName)
		err = writeState("bigbucket/.delete_tables", tablesToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal server error, check logs",
			})
			return
		}
		c.JSON(200, gin.H{
			"success": fmt.Sprintf("Table '%s' marked for deletion", tableName),
		})
	}
}

// Table helper functions

func getTables() (tables []string, tablesToDelete []string, err error) {
	tables = []string{}
	objects, err := store.ListObjects("bigbucket/", "/", 0)
	if err != nil {
		return nil, nil, err
	}

	for _, table := range objects {
		cleanTable := strings.Replace(strings.Replace(table, "bigbucket", "", 1), "/", "", -1)
		if cleanTable != "" {
			tables = append(tables, cleanTable)
		}
	}

	// Remove tables marked for deletion from results
	tablesToDelete = getState("bigbucket/.delete_tables")
	for _, tableToDelete := range tablesToDelete {
		index := search(tables, tableToDelete)
		if index > -1 {
			tables = removeIndex(tables, index)
		}
	}

	return tables, tablesToDelete, nil
}
