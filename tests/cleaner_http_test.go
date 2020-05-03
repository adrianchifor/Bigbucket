package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestCleanerHttp(t *testing.T) {
	if err := deleteTable(); err != nil {
		t.Error(err)
	}
}

func deleteTable() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/table?table=test1", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteTable /api/table DELETE response status code is not 200")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/table")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteTable /api/table GET response status code is not 200")
	}

	defer resp.Body.Close()
	var dataTable map[string][]string
	json.NewDecoder(resp.Body).Decode(&dataTable)
	if err != nil {
		return err
	}

	if len(dataTable["tables"]) != 0 {
		return errors.New("deleteTable table was not marked as deleted")
	}

	resp, err = http.Post("http://127.0.0.1:8081/", "application/json", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteTable cleaner-http / POST response status code is not 200")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/row?table=test1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 404 {
		return errors.New("deleteTable /api/row GET response status code is not 404")
	}

	return nil
}
