package api

import (
	"bigbucket/storage"
	"github.com/gin-gonic/gin"
)

func listColumnFamilies(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}

func createColumnFamily(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}

func deleteColumnFamily(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}
