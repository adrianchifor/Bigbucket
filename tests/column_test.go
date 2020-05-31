package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestColumns(t *testing.T) {
	if err := listColumns(); err != nil {
		t.Error(err)
	}
	if err := listColumnsBadParams(); err != nil {
		t.Error(err)
	}
	if err := deleteColumnBadParams(); err != nil {
		t.Error(err)
	}
}

func listColumns() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/column?table=test1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("listColumns /api/column GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string][]string
	json.NewDecoder(resp.Body).Decode(&data)

	if len(data["columns"]) != 4 || data["columns"][0] != "col1" {
		return errors.New("listColumns columns do not match those set")
	}
	return nil
}

func listColumnsBadParams() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/column")
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("listColumnsBadParams /api/column GET (no table) response status code is not 400")
	}

	return nil
}

func deleteColumnBadParams() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/column", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("deleteColumnBadParams /api/column DELETE (no table) response status code is not 400")
	}

	req, err = http.NewRequest("DELETE", "http://127.0.0.1:8080/api/column?table=test1", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("deleteColumnBadParams /api/column DELETE (no column) response status code is not 400")
	}

	return nil
}
