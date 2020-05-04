package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestRows(t *testing.T) {
	if err := setRows(); err != nil {
		t.Error(err)
	}
	if err := setRowsBadParams(); err != nil {
		t.Error(err)
	}
	if err := readSingleRow(); err != nil {
		t.Error(err)
	}
	if err := readSingleRowColumn(); err != nil {
		t.Error(err)
	}
	if err := readAllRows(); err != nil {
		t.Error(err)
	}
	if err := readRowsWithPrefix(); err != nil {
		t.Error(err)
	}
	if err := readRowsWithColumns(); err != nil {
		t.Error(err)
	}
	if err := readRowsWithLimit(); err != nil {
		t.Error(err)
	}
	if err := readRowsBadParams(); err != nil {
		t.Error(err)
	}
	if err := countRows(); err != nil {
		t.Error(err)
	}
	if err := countRowsBadParams(); err != nil {
		t.Error(err)
	}
	if err := deleteSingleRow(); err != nil {
		t.Error(err)
	}
	if err := deleteRowsPrefix(); err != nil {
		t.Error(err)
	}
	if err := deleteRowsBadParams(); err != nil {
		t.Error(err)
	}
}

func setRows() error {
	reqBody, err := json.Marshal(map[string]string{
		"col1": "qwerty1",
		"col2": "qwerty2",
		"col3": "qwerty3",
		"col4": "qwerty4",
	})
	if err != nil {
		return err
	}

	keys := []string{"key", "rowkey"}
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:8080/api/row?table=test1&key=%s%d", key, i),
				"application/json", bytes.NewBuffer(reqBody))
			if err != nil {
				return err
			}
			if resp.StatusCode != 200 {
				return errors.New("setRows /api/row POST response status code is not 200")
			}
		}
	}

	return nil
}

func setRowsBadParams() error {
	resp, err := http.Post("http://127.0.0.1:8080/api/row", "application/json", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("setRowsBadParams /api/row POST (no table) response status code is not 400")
	}

	resp, err = http.Post("http://127.0.0.1:8080/api/row?table=test1", "application/json",
		bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("setRowsBadParams /api/row POST (no key) response status code is not 400")
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"col1": 1, // Value not a string
	})
	if err != nil {
		return err
	}
	resp, err = http.Post("http://127.0.0.1:8080/api/row?table=test1&key=key0", "application/json",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("setRowsBadParams /api/row POST (bad json, int value) response status code is not 400")
	}

	reqBody, err = json.Marshal(map[string]interface{}{
		"col1/": "test", // Invalid column name
	})
	if err != nil {
		return err
	}
	resp, err = http.Post("http://127.0.0.1:8080/api/row?table=test1&key=key0", "application/json",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("setRowsBadParams /api/row POST (bad json, invalid column) response status code is not 400")
	}

	return nil
}

func readSingleRow() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1&key=key1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readSingleRow /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) != 1 {
		return errors.New("readSingleRow response body doesn't have exactly one row")
	}
	if len(data["key1"]) != 4 {
		return errors.New("readSingleRow response body doesn't have all columns")
	}
	return nil
}

func readSingleRowColumn() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1&key=key1&columns=col2")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readSingleRowColumn /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) != 1 {
		return errors.New("readSingleRowColumn response body doesn't have exactly one row")
	}
	if len(data["key1"]) != 1 {
		return errors.New("readSingleRowColumn response body doesn't have exactly one column")
	}
	if data["key1"]["col2"] != "qwerty2" {
		return errors.New("readSingleRowColumn key1/col2 value incorrect")
	}
	return nil
}

func readAllRows() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readAllRows /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) < 20 {
		return errors.New("readAllRows response body doesn't have all rows")
	}
	if data["key1"]["col2"] != "qwerty2" {
		return errors.New("readAllRows key1/col2 value incorrect")
	}
	return nil
}

func readRowsWithPrefix() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1&prefix=key")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readRowsWithPrefix /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) < 10 {
		return errors.New("readRowsWithPrefix response body doesn't have all rows")
	}
	if data["key1"]["col2"] != "qwerty2" {
		return errors.New("readRowsWithPrefix key1/col2 value incorrect")
	}
	return nil
}

func readRowsWithColumns() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1&columns=col1,col2")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readRowsWithColumns /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) < 20 {
		return errors.New("readRowsWithColumns response body doesn't have all rows")
	}
	for col, _ := range data["key1"] {
		if col != "col1" && col != "col2" {
			return errors.New("readRowsWithColumns got columns other than requested")
		}
	}
	return nil
}

func readRowsWithLimit() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row?table=test1&limit=2")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("readRowsWithLimit /api/row GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if len(data) > 2 {
		return errors.New("readRowsWithLimit response body has more rows than limit")
	}
	return nil
}

func readRowsBadParams() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row")
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("readRowsBadParams /api/row GET (no table) response status code is not 400")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/row?table=test1&key=key1&prefix=key")
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("readRowsBadParams /api/row GET (both key and prefix) response status code is not 400")
	}

	return nil
}

func countRows() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row/count?table=test1")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("countRows /api/row/count GET response status code is not 200")
	}

	defer resp.Body.Close()
	var data map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return err
	}
	if data["rowsCount"] != "20" {
		return errors.New("countRows count doesn't match what was set")
	}
	return nil
}

func countRowsBadParams() error {
	resp, err := http.Get("http://127.0.0.1:8080/api/row/count")
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("countRowsBadParams /api/row/count GET (no table) response status code is not 400")
	}

	return nil
}

func deleteSingleRow() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/row?table=test1&key=key0", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteSingleRow /api/row DELETE response status code is not 200")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/row?table=test1&key=key0")
	if err != nil {
		return err
	}
	if resp.StatusCode != 404 {
		return errors.New("deleteSingleRow /api/row GET response status code is not 404")
	}

	return nil
}

func deleteRowsPrefix() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/row?table=test1&prefix=key", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("deleteRowsPrefix /api/row DELETE response status code is not 200")
	}

	resp, err = http.Get("http://127.0.0.1:8080/api/row?table=test1&prefix=key")
	if err != nil {
		return err
	}
	if resp.StatusCode != 404 {
		return errors.New("deleteRowsPrefix /api/row GET response status code is not 404")
	}

	return nil
}

func deleteRowsBadParams() error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/api/row", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("deleteRowsBadParams /api/row DELETE (no table) response status code is not 400")
	}

	req, err = http.NewRequest("DELETE", "http://127.0.0.1:8080/api/row?table=test1&key=key1&prefix=key", bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return errors.New("deleteRowsBadParams /api/row DELETE (both key and prefix) response status code is not 400")
	}

	return nil
}
