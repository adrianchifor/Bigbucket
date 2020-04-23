package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

func RunServer(port int) {
	r := gin.Default()

	apiRoute := r.Group("/api")
	{
		apiRoute.GET("/table", listTables)
		apiRoute.POST("/table", createTable)
		apiRoute.DELETE("/table", deleteTable)

		apiRoute.GET("/columnfamily", listColumnFamilies)
		apiRoute.POST("/columnfamily", createColumnFamily)
		apiRoute.DELETE("/columnfamily", deleteColumnFamily)

		apiRoute.POST("/rows", getSetRows)
		apiRoute.DELETE("/rows", deleteRows)
	}
	r.GET("/healthz", func(c *gin.Context) {
		c.String(200, "UP")
	})

	r.Run(fmt.Sprintf(":%d", port))
}
