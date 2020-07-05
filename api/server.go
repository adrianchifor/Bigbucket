package api

import (
	"github.com/adrianchifor/Bigbucket/utils"
	"github.com/gin-gonic/gin"
)

// RunServer runs the HTTP server+router for API
func RunServer(port int) {
	router := gin.Default()

	apiRoute := router.Group("/api")
	{
		apiRoute.GET("/table", listTables)
		apiRoute.DELETE("/table", deleteTable)

		apiRoute.GET("/column", listColumns)
		apiRoute.DELETE("/column", deleteColumn)

		apiRoute.GET("/row", getRows)
		apiRoute.GET("/row/count", getRowsCount)
		apiRoute.GET("/row/list", listRows)
		apiRoute.POST("/row", setRow)
		apiRoute.DELETE("/row", deleteRows)
	}
	router.GET("/health", func(c *gin.Context) {
		c.String(200, "UP")
	})

	utils.RunServer(port, router)
}
