package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

func RunServer(port int) {
	r := gin.Default()

	r.Group("/api")
	{
		r.GET("/table", listTables)
		r.POST("/table", createTable)
		r.DELETE("/table", deleteTable)

		r.GET("/columnfamily", listColumnFamilies)
		r.POST("/columnfamily", createColumnFamily)
		r.DELETE("/columnfamily", deleteColumnFamily)

		r.POST("/rows", getSetRows)
		r.DELETE("/rows", deleteRows)
	}
	r.GET("/healthz", func(c *gin.Context) {
		c.String(200, "UP")
	})

	r.Run(fmt.Sprintf(":%d", port))
}
