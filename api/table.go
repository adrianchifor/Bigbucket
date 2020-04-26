package api

import (
	"fmt"
	"log"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

func listTables(c *gin.Context) {
	c.JSON(200, gin.H{"tables": getState("bigbucket/.tables")})
}

func createTable(c *gin.Context) {
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "please provide tableName as a querystring parameter"})
		return
	}

	tables := getState("bigbucket/.tables")
	if search(tables, tableName) > -1 {
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table already exists", tableName)})
	} else {
		tables = append(tables, tableName)
		err := writeState("bigbucket/.tables", tables)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "internal server error, check logs"})
			return
		}
		err = store.WriteObject(fmt.Sprintf("bigbucket/%s/", tableName), []byte(""))
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "internal server error, check logs"})
			return
		}
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table created", tableName)})
	}
}

func deleteTable(c *gin.Context) {
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "please provide tableName as a querystring parameter"})
		return
	}

	tables := getState("bigbucket/.tables")
	index := search(tables, tableName)
	if index == -1 {
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table doesn't exist", tableName)})
	} else {
		tables = removeIndex(tables, index)
		err := writeState("bigbucket/.tables", tables)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "internal server error, check logs"})
			return
		}
		// TODO: need to recurse on objects
		err = store.DeleteObject(fmt.Sprintf("bigbucket/%s/", tableName))
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{"error": "internal server error, check logs"})
			return
		}
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table deleted", tableName)})
	}
}
