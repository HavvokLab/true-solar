package util

import (
	"encoding/json"
	"fmt"

	"github.com/TylerBrock/colorjson"
)

func Recast(from, to interface{}) error {
	switch v := from.(type) {
	case []byte:
		return json.Unmarshal(v, to)
	default:
		buf, err := json.Marshal(from)
		if err != nil {
			return err
		}

		return json.Unmarshal(buf, to)
	}
}

func PrintJSON(obj interface{}) {
	var mapData map[string]interface{}
	if err := Recast(obj, &mapData); err != nil {
		return
	}

	f := colorjson.NewFormatter()
	f.Indent = 4
	s, _ := f.Marshal(mapData)
	fmt.Println(string(s))
}
