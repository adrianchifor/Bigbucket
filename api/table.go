package api

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"bigbucket/store"
	"github.com/gin-gonic/gin"
)

func httpListTables(c *gin.Context) {
	c.JSON(200, gin.H{"tables": getTablesState()})
}

func httpCreateTable(c *gin.Context) {
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "please provide tableName as a querystring parameter"})
		return
	}

	tables := getTablesState()
	if search(tables, tableName) > -1 {
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table already exists", tableName)})
	} else {
		tables = append(tables, tableName)
		err := writeTablesState(tables)
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

func httpDeleteTable(c *gin.Context) {
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(400, gin.H{"error": "please provide tableName as a querystring parameter"})
		return
	}

	tables := getTablesState()
	index := search(tables, tableName)
	if index == -1 {
		c.JSON(200, gin.H{"success": fmt.Sprintf("%s table doesn't exist", tableName)})
	} else {
		tables = removeIndex(tables, index)
		err := writeTablesState(tables)
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

func getTablesState() []string {
	tables := []string{}

	data, err := store.ReadObject("bigbucket/.tables")
	if err != nil {
		return tables
	}
	buf := bytes.NewBuffer(data)
	gob.NewDecoder(buf).Decode(&tables)

	return tables
}

func writeTablesState(tables []string) error {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(tables)
	data := buf.Bytes()

	err := store.WriteObject("bigbucket/.tables", data)
	if err != nil {
		return err
	}

	return nil
}

// Returns index if found, otherwise -1
func search(list []string, a string) int {
	for i, elem := range list {
		if elem == a {
			return i
		}
	}
	return -1
}

func removeIndex(list []string, index int) []string {
	list[index] = list[len(list)-1]
	list[len(list)-1] = ""
	list = list[:len(list)-1]

	return list
}
