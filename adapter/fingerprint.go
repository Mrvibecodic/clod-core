package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// fingerprint identifies a node by how it connects, not by what it is called:
// the description from the config without its name. Renaming a node keeps the
// fingerprint, a new address, port, protocol or key changes it. encoding/json
// writes map keys sorted, so the order of keys in the config does not matter.
// A description that cannot be written as JSON has no fingerprint.
func fingerprint(mapping map[string]any) string {
	described := make(map[string]any, len(mapping))
	for key, value := range mapping {
		if key != "name" {
			described[key] = jsonable(value)
		}
	}
	data, err := json.Marshal(described)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:16]
}

// jsonable turns the maps with untyped keys a YAML decoder may produce into
// maps with string keys, which encoding/json can write.
func jsonable(value any) any {
	switch v := value.(type) {
	case map[any]any:
		m := make(map[string]any, len(v))
		for key, inner := range v {
			m[fmt.Sprint(key)] = jsonable(inner)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(v))
		for key, inner := range v {
			m[key] = jsonable(inner)
		}
		return m
	case []any:
		s := make([]any, len(v))
		for i, inner := range v {
			s[i] = jsonable(inner)
		}
		return s
	}
	return value
}
