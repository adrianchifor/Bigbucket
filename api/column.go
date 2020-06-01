package api

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/adrianchifor/Bigbucket/store"
	"github.com/adrianchifor/Bigbucket/utils"
	"github.com/gin-gonic/gin"
)

func listColumns(c *gin.Context) {
	params, err := parseRequiredRequestParams(c, "table")
	if err != nil {
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
	if utils.Search(tables, params["table"]) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", params["table"]),
		})
		return
	}

	columns, _, err := getColumns(params["table"])
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	c.JSON(200, gin.H{"table": params["table"], "columns": columns})
}

func deleteColumn(c *gin.Context) {
	params, err := parseRequiredRequestParams(c, "table", "column")
	if err != nil {
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
	if utils.Search(tables, params["table"]) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Table '%s' not found or marked for deletion", params["table"]),
		})
		return
	}

	columns, columnsToDelete, err := getColumns(params["table"])
	if err != nil {
		log.Print(err)
		c.JSON(500, gin.H{
			"error": "Internal error, check server logs",
		})
		return
	}

	if utils.Search(columns, params["column"]) == -1 {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("Column '%s' not found or marked for deletion in table '%s'", params["column"], params["table"]),
		})
	} else {
		columnsToDelete = append(columnsToDelete, params["column"])
		err = utils.WriteState(fmt.Sprintf("bigbucket/%s/.delete_columns", params["table"]), columnsToDelete)
		if err != nil {
			log.Print(err)
			c.JSON(500, gin.H{
				"error": "Internal error, check server logs",
			})
			return
		}
		c.JSON(200, gin.H{
			"success": fmt.Sprintf("Column '%s' marked for deletion in table '%s'", params["column"], params["table"]),
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
	indexDelete := utils.Search(objects, fmt.Sprintf("bigbucket/%s/.delete_columns", table))
	if indexDelete > -1 {
		objects = utils.RemoveIndex(objects, indexDelete)
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
	columnsToDelete = utils.GetState(fmt.Sprintf("bigbucket/%s/.delete_columns", table))
	for _, columnToDelete := range columnsToDelete {
		index := utils.Search(columns, columnToDelete)
		if index > -1 {
			columns = utils.RemoveIndex(columns, index)
		}
	}

	sort.Strings(columns)
	return columns, columnsToDelete, nil
}
