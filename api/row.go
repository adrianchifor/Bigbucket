package api

import (
	"bigbucket/storage"
	"github.com/gin-gonic/gin"
)

func getSetRows(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}

func deleteRows(c *gin.Context) {
	c.JSON(200, storage.ListObjects("", "", false))
}
