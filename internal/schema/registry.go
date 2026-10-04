// Package schema loads the event type contracts from schemas/<type>/v<N>.json.
// Adding a directory there is how a team adds an event type; the service has
// no per-type code.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	typeName    = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	versionFile = regexp.MustCompile(`^v([1-9][0-9]*)\.json$`)
)

// Meta is the platform metadata every schema carries next to its JSON Schema
// keywords. It drives ownership, review routing and retention.
type Meta struct {
	Owner         string `json:"x-owner"`
	ContainsPII   bool   `json:"x-contains-pii"`
	RetentionDays int    `json:"x-retention-days"`
}

type Info struct {
	Type    string `json:"event_type"`
	Version int    `json:"schema_version"`
	Meta
}

type entry struct {
	Info
	schema *jsonschema.Schema
}

type Registry struct {
	entries map[string]map[int]entry
}

// Load compiles every schema under dir. It fails on the first bad file: a
// broken contract should stop the deploy (and fail CI before that), not
// surface as rejected events in production.
func Load(dir string) (*Registry, error) {
	types, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	r := &Registry{entries: map[string]map[int]entry{}}
	for _, t := range types {
		if !t.IsDir() {
			continue
		}
		if !typeName.MatchString(t.Name()) {
			return nil, fmt.Errorf("%s: event type must be snake_case", t.Name())
		}
		files, err := os.ReadDir(filepath.Join(dir, t.Name()))
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			m := versionFile.FindStringSubmatch(f.Name())
			if m == nil {
				continue // examples, docs
			}
			version, _ := strconv.Atoi(m[1])
			path := filepath.Join(dir, t.Name(), f.Name())
			e, err := compile(path)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			e.Type, e.Version = t.Name(), version
			if r.entries[e.Type] == nil {
				r.entries[e.Type] = map[int]entry{}
			}
			r.entries[e.Type][version] = e
		}
	}
	return r, nil
}

func compile(path string) (entry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return entry{}, err
	}
	var meta Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return entry{}, err
	}
	if meta.Owner == "" {
		return entry{}, fmt.Errorf("x-owner is required")
	}
	if meta.RetentionDays <= 0 {
		return entry{}, fmt.Errorf("x-retention-days must be positive")
	}

	var tree map[string]any
	if err := json.Unmarshal(raw, &tree); err != nil {
		return entry{}, err
	}
	if _, ok := tree["x-contains-pii"].(bool); !ok {
		return entry{}, fmt.Errorf("x-contains-pii is required (true or false), so every type makes the call explicitly")
	}
	if err := checkKeywords("", tree, true); err != nil {
		return entry{}, err
	}

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return entry{}, err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat() // otherwise "format" is only an annotation and "not a date" passes
	if err := c.AddResource(path, doc); err != nil {
		return entry{}, err
	}
	sch, err := c.Compile(path)
	if err != nil {
		return entry{}, err
	}
	return entry{Info: Info{Meta: meta}, schema: sch}, nil
}

// Contracts use a subset of JSON Schema: the keywords Compare knows how to
// check for compatibility. Anything else (allOf, $ref, if/then...) is
// rejected, because Compare would not notice it changing.
//
// Every object must also set additionalProperties: false. Then an undeclared
// field can never be in the data already, and adding it later as an optional
// property can't reject anything a producer sends today.
var (
	rootOnly = map[string]bool{"$schema": true, "x-owner": true, "x-contains-pii": true, "x-retention-days": true}
	keywords = map[string]bool{
		"title": true, "description": true, "examples": true, "default": true,
		"type": true, "properties": true, "required": true, "enum": true, "const": true, "items": true,
		"additionalProperties": true, "minimum": true, "maximum": true, "exclusiveMinimum": true,
		"exclusiveMaximum": true, "multipleOf": true, "minLength": true, "maxLength": true,
		"pattern": true, "format": true, "minItems": true, "maxItems": true,
	}
)

func checkKeywords(path string, node map[string]any, root bool) error {
	if node["type"] == "object" && node["additionalProperties"] != false {
		return fmt.Errorf("%sobjects must set \"additionalProperties\": false", at(path))
	}
	for k, v := range node {
		if !keywords[k] && !(root && rootOnly[k]) {
			return fmt.Errorf("%skeyword %q is not supported in event contracts, because its compatibility can't be checked", at(path), k)
		}
		switch k {
		case "properties":
			props, _ := v.(map[string]any)
			for name, p := range props {
				child, ok := p.(map[string]any)
				if !ok {
					return fmt.Errorf("%sproperty %q must be an object schema", at(path), name)
				}
				if err := checkKeywords(path+"."+name, child, false); err != nil {
					return err
				}
			}
		case "items":
			child, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("%sitems must be an object schema", at(path))
			}
			if err := checkKeywords(path+"[]", child, false); err != nil {
				return err
			}
		case "additionalProperties":
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("%sadditionalProperties must be true or false", at(path))
			}
		}
	}
	return nil
}

func at(path string) string {
	if path == "" {
		return ""
	}
	return "payload" + path + ": "
}

// Validate checks a payload against the named contract.
func (r *Registry) Validate(eventType string, version int, payload []byte) error {
	versions, ok := r.entries[eventType]
	if !ok {
		return fmt.Errorf("unknown event_type %q (see GET /v1/event-types)", eventType)
	}
	e, ok := versions[version]
	if !ok {
		return fmt.Errorf("event_type %q has no schema_version %d", eventType, version)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if err := e.schema.Validate(doc); err != nil {
		// Drop the library's first line, which names the schema's file path.
		msg := err.Error()
		if i := strings.Index(msg, "\n"); i >= 0 {
			msg = strings.TrimSpace(msg[i+1:])
		}
		return fmt.Errorf("payload does not match %s/v%d: %s", eventType, version, msg)
	}
	return nil
}

// List returns every registered contract, sorted, for discovery.
func (r *Registry) List() []Info {
	var out []Info
	for _, versions := range r.entries {
		for _, e := range versions {
			out = append(out, e.Info)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return out[i].Version < out[j].Version
	})
	return out
}
