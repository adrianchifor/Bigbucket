package api

import (
	"bytes"
	"encoding/gob"

	"bigbucket/store"
)

func getState(object string) []string {
	state := []string{}

	data, err := store.ReadObject(object)
	if err != nil {
		return state
	}
	buf := bytes.NewBuffer(data)
	gob.NewDecoder(buf).Decode(&state)

	return state
}

func writeState(object string, state []string) error {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(state)
	data := buf.Bytes()

	err := store.WriteObject(object, data)
	if err != nil {
		return err
	}

	return nil
}

// Returns index if found, otherwise -1
func search(list []string, a string) int {
	for i, elem := range list {
		if elem == a {
			return i
		}
	}
	return -1
}

func removeIndex(list []string, index int) []string {
	list[index] = list[len(list)-1]
	list[len(list)-1] = ""
	list = list[:len(list)-1]

	return list
}

func mergeMaps(maps ...map[string]string) map[string]string {
	mergedMap := make(map[string]string)
	for _, innerMap := range maps {
		for k, v := range innerMap {
			mergedMap[k] = v
		}
	}

	return mergedMap
}
