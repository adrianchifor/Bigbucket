package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

var (
	invalidChars = []string{"\n", "\r", "\t", "\b", "#", "[", "]", "?", "/"}
)

func parseRequiredRequestParams(c *gin.Context, params ...string) (map[string]string, error) {
	parsedParams := make(map[string]string)
	for _, param := range params {
		paramValue := strings.TrimSpace(c.Query(param))
		if paramValue == "" {
			c.JSON(400, gin.H{
				"error": fmt.Sprintf("Please provide '%s' as a querystring parameter", param),
			})
			return nil, errors.New(fmt.Sprintf("Failed to parse '%s' querystring parameter", param))
		}
		if err := validateParam(c, paramValue); err != nil {
			return nil, err
		}

		parsedParams[param] = paramValue
	}

	return parsedParams, nil
}

func parseOptionalRequestParams(c *gin.Context, params ...string) (map[string]string, error) {
	parsedParams := make(map[string]string)
	for _, param := range params {
		paramValue := strings.TrimSpace(c.Query(param))
		if err := validateParam(c, paramValue); err != nil {
			return nil, err
		}

		parsedParams[param] = paramValue
	}

	return parsedParams, nil
}

func parseExclusiveRequestParams(c *gin.Context, firstParam string, secondParam string) (string, string, error) {
	firstParamVal := strings.TrimSpace(c.Query(firstParam))
	secondParamVal := strings.TrimSpace(c.Query(secondParam))

	if firstParamVal != "" && firstParamVal != "" {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("Please provide only one of '%s' or '%s' as a querystring parameter", firstParam, secondParam),
		})
		return "", "", errors.New(fmt.Sprintf("Failed to parse '%s, %s' querystring parameters", firstParam, secondParam))
	}
	if err := validateParam(c, firstParamVal); err != nil {
		return "", "", err
	}
	if err := validateParam(c, secondParamVal); err != nil {
		return "", "", err
	}

	return firstParamVal, secondParamVal, nil
}

func validateParam(c *gin.Context, paramValue string) error {
	if !isObjectNameValid(paramValue) {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("parameters cannot start with '.' nor contain the following characters: %s", invalidChars),
		})
		return errors.New("Failed to validate querystring parameter")
	}
	return nil
}

func isObjectNameValid(object string) bool {
	if strings.HasPrefix(object, ".") {
		return false
	}

	for _, char := range invalidChars {
		if strings.Contains(object, char) {
			return false
		}
	}

	return true
}
