package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// fingerprint identifies a node by how it connects, not by what it is called:
// the description from the config without its name. Renaming a node keeps the
// fingerprint, a new address, port, protocol or key changes it. encoding/json
// writes map keys sorted, so the order of keys in the config does not matter.
// A description that cannot be written as JSON has no fingerprint, and neither
// has an entry that leads to no server (see leadsToAServer): a check through
// it would tell nothing about a server.
func fingerprint(mapping map[string]any) string {
	if !leadsToAServer(mapping) {
		return ""
	}
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

// serverlessTypes go nowhere but the local stack: a subscription may list them
// among its nodes («without VPN», «block»).
var serverlessTypes = map[string]bool{
	"direct": true, "reject": true, "reject-drop": true, "pass": true, "dns": true,
}

// credentialKeys are the keys that carry a node's secret.
var credentialKeys = []string{"uuid", "password", "psk", "private-key", "auth", "auth-str", "token"}

const nilUUID = "00000000-0000-0000-0000-000000000000"

// leadsToAServer tells a node from a serverless entry and from a placeholder
// a panel sends in place of a server — an unspecified address, a nil UUID, or
// a port of 0 or 1 without a key. The same rule the clients use for
// placeholders.
func leadsToAServer(mapping map[string]any) bool {
	kind, _ := mapping["type"].(string)
	if serverlessTypes[strings.ToLower(strings.TrimSpace(kind))] {
		return false
	}
	switch host := mapping["server"].(type) {
	case string:
		switch strings.TrimSpace(host) {
		case "", "0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0":
			return false
		}
	case nil:
		if _, present := mapping["server"]; present {
			return false
		}
	}
	if id, ok := mapping["uuid"].(string); ok && strings.EqualFold(strings.TrimSpace(id), nilUUID) {
		return false
	}
	return !(deadPort(mapping) && !hasCredentials(mapping))
}

func deadPort(mapping map[string]any) bool {
	value, present := mapping["port"]
	if !present {
		return false
	}
	switch port := value.(type) {
	case int:
		return port <= 1
	case int64:
		return port <= 1
	case uint64:
		return port <= 1
	case float64:
		return port <= 1
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(port))
		return err != nil || parsed <= 1
	}
	return true
}

func hasCredentials(mapping map[string]any) bool {
	for _, key := range credentialKeys {
		if value, ok := mapping[key].(string); ok && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}
