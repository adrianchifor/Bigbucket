package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestTables(t *testing.T) {
	if err := listTables(); err != nil {
		t.Error(err)
	}
	if err := deleteTableBadParams(); err != nil {
		t.Error(err)
	}
}

func listTables() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/table")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("listTables /api/table GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string][]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}

	if len(data["tables"]) != 1 || data["tables"][0] != "test1" {
		return errors.New("listTables tables do not match those set")
	}
	return nil
}

func deleteTableBadParams() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/table", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("deleteTableBadParams /api/table DELETE (no table) response status code is not 400")
	}

	return nil
}
