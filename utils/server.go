package utils

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func RunServer(port int, router *gin.Engine) {
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	server, listenAddr := newServer(port, router)
	go serverGracefulShutdown(server, quit, done)

	log.Println("HTTP server is ready to handle requests at", listenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server could not listen on %s: %v\n", listenAddr, err)
	}

	<-done
	log.Println("HTTP server stopped")
}

func newServer(port int, router *gin.Engine) (*http.Server, string) {
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)

	ginMode := os.Getenv("GIN_MODE")
	if ginMode == "release" {
		listenAddr = fmt.Sprintf(":%d", port)
	}

	return &http.Server{
		Addr:    listenAddr,
		Handler: router,
	}, listenAddr
}

func serverGracefulShutdown(server *http.Server, quit <-chan os.Signal, done chan<- bool) {
	<-quit
	log.Println("HTTP server is shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server.SetKeepAlivesEnabled(false)
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Could not gracefully shutdown the HTTP server: %v\n", err)
	}
	close(done)
}
