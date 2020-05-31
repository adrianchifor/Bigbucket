package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestCleaner(t *testing.T) {
	if err := deleteColumn(); err != nil {
		t.Error(err)
	}
}

func deleteColumn() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/column?table=test1&column=col1", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteColumn /api/column DELETE response status code is not 200")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/column?table=test1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteColumn /api/column GET response status code is not 200")
	}

	defer resp.Body.Close()
	var dataColumn map[string][]string
	json.NewDecoder(resp.Body).Decode(&dataColumn)

	if len(dataColumn["columns"]) != 3 || dataColumn["columns"][0] != "col2" {
		return errors.New("deleteColumn column was not marked as deleted")
	}

	// Wait 6s for cleaner to trigger
	time.Sleep(6 * time.Second)

	resp, err = http.Get("http://127.0.0.1:8080/api/row?table=test1&key=rowkey1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteColumn /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var dataRow map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&dataRow)

	if len(dataRow["rowkey1"]) != 3 {
		return errors.New("deleteColumn response body still has all columns")
	}

	return nil
}
