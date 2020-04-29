package api

import (
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
)

func RunServer(port int) {
	r := gin.Default()

	apiRoute := r.Group("/api")
	{
		apiRoute.GET("/table", listTables)
		apiRoute.DELETE("/table", deleteTable)

		apiRoute.GET("/column", listColumns)
		apiRoute.DELETE("/column", deleteColumn)

		apiRoute.GET("/rows", getRows)
		apiRoute.POST("/row", setRow)
		apiRoute.DELETE("/row", deleteRow)
	}
	r.GET("/health", func(c *gin.Context) {
		c.String(200, "UP")
	})

	ginMode := os.Getenv("GIN_MODE")
	if ginMode == "release" {
		r.Run(fmt.Sprintf(":%d", port))
	} else {
		r.Run(fmt.Sprintf("127.0.0.1:%d", port))
	}
}
