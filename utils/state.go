package utils

import (
	"bytes"
	"encoding/gob"

	"github.com/adrianchifor/Bigbucket/store"
)

// GetState gets object content as string[]
func GetState(object string) []string {
	state := []string{}

	data, err := store.ReadObject(object)
	if err != nil {
		return state
	}
	buf := bytes.NewBuffer(data)
	gob.NewDecoder(buf).Decode(&state)

	return state
}

// WriteState writes string[] to object
func WriteState(object string, state []string) error {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(state)
	data := buf.Bytes()

	err := store.WriteObject(object, data)
	if err != nil {
		return err
	}

	return nil
}
