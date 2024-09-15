package model

import "encoding/json"

type ApiErrorResponse map[string]any

func (e ApiErrorResponse) String() string {
	bytes, err := json.Marshal(e)
	if err != nil {
		return "cannot define error response"
	}

	return string(bytes)
}
