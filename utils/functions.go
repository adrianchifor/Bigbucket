package utils

import (
	"strings"
)

// Linear search, returns first index where found, otherwise -1
func Search(list []string, a string) int {
	for i, elem := range list {
		if elem == a {
			return i
		}
	}
	return -1
}

// Remove item from slice at index
func RemoveIndex(list []string, index int) []string {
	list[index] = list[len(list)-1]
	list[len(list)-1] = ""
	list = list[:len(list)-1]

	return list
}

// Merge multiple maps into one; duplicate k-v in subsequent maps will override previous ones
func MergeMaps(maps ...map[string]string) map[string]string {
	mergedMap := make(map[string]string)
	for _, innerMap := range maps {
		for k, v := range innerMap {
			mergedMap[k] = v
		}
	}

	return mergedMap
}

// Filter out 'bigbucket' and '/' from tables []string
func CleanupTables(tables []string) []string {
	cleanTables := []string{}
	for _, table := range tables {
		cleanTable := strings.Replace(strings.Replace(table, "bigbucket", "", 1), "/", "", -1)
		if cleanTable != "" {
			cleanTables = append(cleanTables, cleanTable)
		}
	}

	return cleanTables
}
