package schema

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Findings from comparing a published schema version with a proposed edit.
type Findings struct {
	Breaking []string // must ship as a new version instead
	Review   []string // allowed, but the platform team should look
	Notes    []string // allowed, worth telling consumers
}

// Compare checks that newRaw can replace oldRaw under the same version number.
// An edit to a published version has to keep both sides working: producers
// that send today's events must still pass validation, and consumers that
// read today's events must still find what they rely on. So adding an
// optional field or an enum value is fine, because the contract tells
// consumers to ignore unknown fields and handle unknown enum values.
// Removing a field, changing a type, changing what is required or changing a
// constraint is not.
func Compare(oldRaw, newRaw []byte) (Findings, error) {
	var oldDoc, newDoc map[string]any
	if err := json.Unmarshal(oldRaw, &oldDoc); err != nil {
		return Findings{}, fmt.Errorf("old schema: %w", err)
	}
	if err := json.Unmarshal(newRaw, &newDoc); err != nil {
		return Findings{}, fmt.Errorf("new schema: %w", err)
	}
	var f Findings
	compareNode("payload", oldDoc, newDoc, &f)

	if oldDoc["x-owner"] != newDoc["x-owner"] {
		f.Review = append(f.Review, fmt.Sprintf("ownership moves from %v to %v", oldDoc["x-owner"], newDoc["x-owner"]))
	}
	// While platformcheck blocks personal data on telemetry.v1, these two
	// can't trigger. They are the rules for once PII has its own topic.
	switch {
	case newDoc["x-contains-pii"] == true && oldDoc["x-contains-pii"] != true:
		f.Review = append(f.Review, "schema now contains personal data")
	case newDoc["x-contains-pii"] != true && oldDoc["x-contains-pii"] == true:
		f.Review = append(f.Review, "personal-data flag removed; confirm the data really is gone")
	}
	oldDays, _ := oldDoc["x-retention-days"].(float64)
	newDays, _ := newDoc["x-retention-days"].(float64)
	switch {
	case newDays > oldDays && newDays > MaxSelfServeRetentionDays:
		f.Review = append(f.Review, fmt.Sprintf("retention raised to %.0f days, above the self-serve limit of %d", newDays, MaxSelfServeRetentionDays))
	case newDays < oldDays:
		// The S3 lifecycle rule follows this number, so lowering it deletes
		// archive data that other teams (and the data platform) may rely on.
		f.Review = append(f.Review, fmt.Sprintf("retention lowered from %.0f to %.0f days; archived events older than that will be deleted", oldDays, newDays))
	}
	return f, nil
}

// ReviewNew lists what needs platform review in a schema version that did not
// exist before.
func ReviewNew(raw []byte, isFirstVersion bool) ([]string, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var review []string
	if doc["x-contains-pii"] == true {
		review = append(review, "new schema contains personal data")
	}
	if days, _ := doc["x-retention-days"].(float64); days > MaxSelfServeRetentionDays {
		review = append(review, fmt.Sprintf("retention of %.0f days is above the self-serve limit of %d", days, MaxSelfServeRetentionDays))
	}
	if !isFirstVersion {
		review = append(review, "new major version: consumers need a migration plan and a date to drop the old one")
	}
	return review, nil
}

// MaxSelfServeRetentionDays covers 13 months of meter data for billing
// disputes. Anything longer is a storage cost and a GDPR question.
const MaxSelfServeRetentionDays = 400

func compareNode(path string, oldN, newN map[string]any, f *Findings) {
	if !equal(oldN["type"], newN["type"]) {
		f.Breaking = append(f.Breaking, fmt.Sprintf("%s: type changed from %v to %v", path, oldN["type"], newN["type"]))
		return
	}

	oldEnum, _ := oldN["enum"].([]any)
	newEnum, hasNewEnum := newN["enum"].([]any)
	if oldEnum == nil && hasNewEnum {
		f.Breaking = append(f.Breaking, path+": enum added to a field that accepted any value")
	}
	if oldEnum != nil && !hasNewEnum {
		f.Breaking = append(f.Breaking, path+": enum removed, so consumers can get values they have never seen")
	}
	for _, v := range oldEnum {
		if hasNewEnum && !slices.ContainsFunc(newEnum, func(n any) bool { return equal(n, v) }) {
			f.Breaking = append(f.Breaking, fmt.Sprintf("%s: enum value %v removed", path, v))
		}
	}
	if oldEnum != nil {
		for _, v := range newEnum {
			if !slices.ContainsFunc(oldEnum, func(o any) bool { return equal(o, v) }) {
				f.Notes = append(f.Notes, fmt.Sprintf("%s: enum value %v added; consumers with exhaustive switches should handle unknown values", path, v))
			}
		}
	}

	for _, kw := range constraints {
		if !equal(oldN[kw], newN[kw]) {
			f.Breaking = append(f.Breaking, fmt.Sprintf("%s: %s changed from %v to %v", path, kw, orNone(oldN[kw]), orNone(newN[kw])))
		}
	}

	oldReq, newReq := stringSet(oldN["required"]), stringSet(newN["required"])
	for r := range newReq {
		if !oldReq[r] {
			f.Breaking = append(f.Breaking, fmt.Sprintf("%s.%s: became required (producers may not send it)", path, r))
		}
	}
	for r := range oldReq {
		if !newReq[r] {
			f.Breaking = append(f.Breaking, fmt.Sprintf("%s.%s: no longer required (consumers may rely on it)", path, r))
		}
	}

	oldProps, _ := oldN["properties"].(map[string]any)
	newProps, _ := newN["properties"].(map[string]any)
	for name, op := range oldProps {
		np, ok := newProps[name]
		if !ok {
			f.Breaking = append(f.Breaking, fmt.Sprintf("%s.%s: removed", path, name))
			continue
		}
		om, _ := op.(map[string]any)
		nm, _ := np.(map[string]any)
		compareNode(path+"."+name, om, nm, f)
	}
	for name := range newProps {
		if _, ok := oldProps[name]; !ok {
			f.Notes = append(f.Notes, fmt.Sprintf("%s.%s: added (optional)", path, name))
		}
	}

	oi, hadItems := oldN["items"].(map[string]any)
	ni, hasItems := newN["items"].(map[string]any)
	switch {
	case hadItems && hasItems:
		compareNode(path+"[]", oi, ni, f)
	case hadItems != hasItems:
		f.Breaking = append(f.Breaking, path+": items added or removed, which changes what the array may hold")
	}
}

// Tightening any of these rejects events producers send today; loosening them
// lets through values consumers have never seen. Either way it is a new version.
var constraints = []string{
	"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
	"minLength", "maxLength", "pattern", "format",
	"minItems", "maxItems", "additionalProperties", "const",
}

func orNone(v any) any {
	if v == nil {
		return "none"
	}
	return v
}

func equal(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

func stringSet(v any) map[string]bool {
	out := map[string]bool{}
	list, _ := v.([]any)
	for _, s := range list {
		if str, ok := s.(string); ok {
			out[str] = true
		}
	}
	return out
}
