package api

import (
	"fmt"
	"log"
	"strings"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

func listTables(c *gin.Context) {
	tables, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{"error": "Internal server error, check logs"})
		return
	}

	// Remove tables marked for deletion from results
	tablesToDelete := getState("bigbucket/.delete_tables")
	for _, tableToDelete := range tablesToDelete {
		index := search(tables, tableToDelete)
		if index > -1 {
			tables = removeIndex(tables, index)
		}
	}

	c.JSON(200, gin.H{"tables": tables})
}

func deleteTable(c *gin.Context) {
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "Please provide tableName as a querystring parameter"})
		return
	}

	tablesToDelete := getState("bigbucket/.delete_tables")
	if search(tablesToDelete, tableName) > -1 {
		c.JSON(200, gin.H{"success": fmt.Sprintf("'%s' table already marked for deletion", tableName)})
	} else {
		tables, err := getTables()
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "Internal server error, check logs"})
			return
		}

		if search(tables, tableName) == -1 {
			c.JSON(404, gin.H{"error": fmt.Sprintf("'%s' table not found", tableName)})
			return
		}

		tablesToDelete = append(tablesToDelete, tableName)
		err = writeState("bigbucket/.delete_tables", tablesToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "Internal server error, check logs"})
			return
		}
		c.JSON(200, gin.H{"success": fmt.Sprintf("'%s' table marked for deletion", tableName)})
	}
}

func getTables() ([]string, error) {
	objects, err := store.ListObjects("bigbucket/", "/")
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, table := range objects {
		cleanTable := strings.Replace(strings.Replace(table, "bigbucket", "", 1), "/", "", -1)
		if cleanTable != "" {
			tables = append(tables, cleanTable)
		}
	}

	return tables, nil
}
