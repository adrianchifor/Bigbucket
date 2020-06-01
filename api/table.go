package api

import (
	"fmt"
	"log"
	"sort"

	"github.com/adrianchifor/Bigbucket/store"
	"github.com/adrianchifor/Bigbucket/utils"
	"github.com/gin-gonic/gin"
)

func listTables(c *gin.Context) {
	tables, _, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	c.JSON(200, gin.H{"tables": tables})
}

func deleteTable(c *gin.Context) {
	params, err := parseRequiredRequestParams(c, "table")
	if err != nil {
		return
	}

	tables, tablesToDelete, err := getTables()
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	if utils.Search(tables, params["table"]) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", params["table"]),
		})
	} else {
		tablesToDelete = append(tablesToDelete, params["table"])
		err = utils.WriteState("bigbucket/.delete_tables", tablesToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal error, check server logs",
			})
			return
		}
		c.JSON(200, gin.H{
			"success": fmt.Sprintf("Table '%s' marked for deletion", params["table"]),
		})
	}
}

func getTables() (tables []string, tablesToDelete []string, err error) {
	objects, err := store.ListObjects("bigbucket/", "/", 0)
	if err != nil {
		return nil, nil, err
	}
	tables = utils.CleanupTables(objects)

	// Remove tables marked for deletion from results
	tablesToDelete = utils.GetState("bigbucket/.delete_tables")
	for _, tableToDelete := range tablesToDelete {
		index := utils.Search(tables, tableToDelete)
		if index > -1 {
			tables = utils.RemoveIndex(tables, index)
		}
	}

	sort.Strings(tables)
	return tables, tablesToDelete, nil
}
