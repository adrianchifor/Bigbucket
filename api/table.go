package api

import (
	"bigbucket/storage"
	"github.com/gin-gonic/gin"
)

func listTables(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}

func createTable(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}

func deleteTable(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}
