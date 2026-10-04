// Command platformcheck is the guardrail CI runs on every pull request that
// touches schemas/ or consumers/. It replaces a human review for the common
// case and asks for one only when a change is sensitive.
//
//	go run ./tools/platformcheck                          # validate the repo
//	go run ./tools/platformcheck -base origin/main \
//	    -review-out review.md -comment-out comment.md     # also diff against main
//
// Exit status 1 means the change cannot merge as is. A non-empty review file
// means it can merge once cloud-platform approves. The comment file holds
// everything worth telling the author, notes included, for the PR comment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mekaan/try-telemetry/internal/ingest"
	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

// MaxSelfServeRPS is the read API budget a team gets without asking.
const MaxSelfServeRPS = 100

// MaxRPS is the most any single client can have, reviewed or not.
const MaxRPS = 2000

type consumer struct {
	Team           string   `yaml:"team"`
	Owner          string   `yaml:"owner"`
	ServiceAccount string   `yaml:"service_account"`
	Access         string   `yaml:"access"`
	RateLimitRPS   int      `yaml:"rate_limit_rps"`
	EventTypes     []string `yaml:"event_types"`
}

var (
	versionPath  = regexp.MustCompile(`^schemas/([a-z][a-z0-9_]*)/v([1-9][0-9]*)\.json$`)
	ownerRef     = regexp.MustCompile(`^@[a-z0-9-]+/[a-z0-9-]+$`)
	consumerName = regexp.MustCompile(`^[a-z][a-z0-9-]{1,30}$`)
	k8sSA        = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?/[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

// Service accounts in these namespaces belong to the platform or the cluster.
// A registration naming one would hand a team's role to a platform workload.
var reservedNamespaces = []string{"telemetry", "kube-system", "kube-public", "argocd", "external-secrets", "kyverno"}

// The file name is a consumer's identity: it names the IAM role and the
// consumer group prefix (<name>.*). These names belong to the platform's own
// workloads, so no registration may take them.
var reservedNames = []string{"telemetry", "platform", "cloud-platform", "ingest", "admin"}

type report struct {
	errors, review, notes []string
}

func (r *report) fail(format string, a ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, a...))
}
func (r *report) ask(format string, a ...any) { r.review = append(r.review, fmt.Sprintf(format, a...)) }
func (r *report) notice(format string, a ...any) {
	r.notes = append(r.notes, fmt.Sprintf(format, a...))
}

func main() {
	schemaDir := flag.String("schemas", "schemas", "schema directory")
	consumerDir := flag.String("consumers", "consumers", "consumer registration directory")
	base := flag.String("base", "", "git ref to diff against (enables compatibility and review checks)")
	reviewOut := flag.String("review-out", "", "write reasons for platform review to this file")
	commentOut := flag.String("comment-out", "", "write notes and review reasons for the PR comment to this file")
	flag.Parse()

	var r report
	reg, err := schema.Load(*schemaDir)
	if err != nil {
		r.fail("schemas: %v", err)
		finish(&r, *reviewOut, *commentOut)
	}
	checkSchemas(&r, reg, *schemaDir)
	consumers := checkConsumers(&r, reg, *consumerDir)
	if *base != "" {
		diffAgainst(&r, reg, consumers, *base)
	}
	finish(&r, *reviewOut, *commentOut)
}

// checkSchemas runs each type's example through the real ingest code, so the
// contract a team publishes is proven to be accepted by the pipeline.
func checkSchemas(r *report, reg *schema.Registry, dir string) {
	// Every file under schemas/ must belong to an event type directory, where
	// x-owner says who approves it. A loose file would have no owner.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				r.fail("%s: only <event_type>/ directories belong in schemas/", filepath.Join(dir, e.Name()))
			}
		}
	}
	seen := map[string]bool{}
	for _, info := range reg.List() {
		if !ownerRef.MatchString(info.Owner) {
			r.fail("%s/v%d: x-owner %q must be a GitHub team like @acme/charging-core", info.Type, info.Version, info.Owner)
		}
		// Stream consumers can read everything on telemetry.v1, so personal
		// data can't go there. Until PII types get their own topic with its
		// own IAM, a review couldn't actually restrict who reads it.
		if info.ContainsPII {
			r.fail("%s/v%d: personal data is not accepted on telemetry.v1 yet, because every stream consumer can read that topic; talk to cloud-platform about a separate topic", info.Type, info.Version)
		}
		if seen[info.Type] {
			continue
		}
		seen[info.Type] = true
		path := filepath.Join(dir, info.Type, "example.json")
		raw, err := os.ReadFile(path)
		if err != nil {
			r.fail("%s: every event type needs an example.json (used as a contract test and in the docs)", info.Type)
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		var e ingest.Envelope
		if err := dec.Decode(&e); err != nil {
			r.fail("%s: invalid envelope: %v", path, err)
			continue
		}
		if e.EventType != info.Type {
			r.fail("%s: event_type is %q, expected %q", path, e.EventType, info.Type)
		}
		// Examples carry fixed timestamps, so judge each at its own time.
		proc := &ingest.Processor{Schemas: reg, Store: discard{}, Now: func() time.Time { return e.OccurredAt }}
		if _, _, err := proc.Process(context.Background(), e); err != nil {
			r.fail("%s: rejected by ingest: %v", path, err)
		}
	}
}

func checkConsumers(r *report, reg *schema.Registry, dir string) map[string]consumer {
	known := map[string]schema.Info{}
	for _, info := range reg.List() {
		known[info.Type] = info // latest version wins; PII flag is per type in practice
	}
	out := map[string]consumer{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		r.fail("%s: %v", dir, err)
		return out
	}
	var files []string
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
			r.fail("%s: consumers/ holds only <name>.yaml registrations; anything else would be silently ignored by Terraform", path)
			continue
		}
		files = append(files, path)
	}
	serviceAccounts := map[string]string{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			r.fail("%s: %v", path, err)
			continue
		}
		var c consumer
		dec := yaml.NewDecoder(bytes.NewReader(raw))
		dec.KnownFields(true)
		if err := dec.Decode(&c); err != nil {
			r.fail("%s: %v", path, err)
			continue
		}
		name := strings.TrimSuffix(filepath.Base(path), ".yaml")
		if other, ok := serviceAccounts[c.ServiceAccount]; ok && c.ServiceAccount != "" {
			r.fail("%s: service account %s is already registered by %s; one registration per workload", path, c.ServiceAccount, other)
		}
		serviceAccounts[c.ServiceAccount] = path
		if !consumerName.MatchString(name) {
			r.fail("%s: file name must be lowercase letters, digits and dashes; it becomes the role name and consumer group prefix", path)
		}
		for _, reserved := range reservedNames {
			if name == reserved || strings.HasPrefix(name, reserved+"-") {
				r.fail("%s: %q is reserved for platform workloads", path, reserved)
			}
		}
		switch {
		case c.Team == "":
			r.fail("%s: team is required", path)
		case !k8sSA.MatchString(c.ServiceAccount):
			r.fail("%s: service_account must be namespace/name, like billing/meter-aggregator", path)
		case slices.Contains(reservedNamespaces, strings.Split(c.ServiceAccount, "/")[0]):
			r.fail("%s: namespace %s belongs to the platform", path, strings.Split(c.ServiceAccount, "/")[0])
		case c.RateLimitRPS < 0 || c.RateLimitRPS > MaxRPS:
			r.fail("%s: rate_limit_rps must be between 0 (the default of 20) and %d", path, MaxRPS)
		case !ownerRef.MatchString(c.Owner):
			r.fail("%s: owner must be a GitHub team like @acme/billing", path)
		case c.Access != "stream" && c.Access != "api":
			r.fail("%s: access must be stream (own Kafka consumer group) or api (read API client)", path)
		case len(c.EventTypes) == 0:
			r.fail("%s: list the event_types you read, so owners know who depends on them", path)
		}
		for _, t := range c.EventTypes {
			if _, ok := known[t]; !ok {
				r.fail("%s: unknown event type %q", path, t)
			}
		}
		out[name] = c
	}
	return out
}

func diffAgainst(r *report, reg *schema.Registry, consumers map[string]consumer, base string) {
	changed, err := git("diff", "--name-status", "--no-renames", base, "--", "schemas", "consumers")
	if err != nil {
		r.fail("git diff against %s: %v", base, err)
		return
	}
	pii := map[string]bool{}
	for _, info := range reg.List() {
		pii[info.Type] = pii[info.Type] || info.ContainsPII
	}

	for _, line := range strings.Split(strings.TrimSpace(changed), "\n") {
		status, path, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		if m := versionPath.FindStringSubmatch(path); m != nil {
			checkSchemaChange(r, base, status, path, m[1])
			continue
		}
		if strings.HasPrefix(path, "consumers/") && strings.HasSuffix(path, ".yaml") && status != "D" {
			c := consumers[strings.TrimSuffix(filepath.Base(path), ".yaml")]
			if status == "M" {
				if oldRaw, err := git("show", base+":"+path); err == nil {
					var old consumer
					if yaml.Unmarshal([]byte(oldRaw), &old) == nil && old.Owner != c.Owner {
						r.ask("%s: ownership moves from %s to %s", path, old.Owner, c.Owner)
					}
				}
			}
			// Can't trigger while checkSchemas blocks PII on telemetry.v1; this
			// is the rule for once personal data has its own topic.
			for _, t := range c.EventTypes {
				if pii[t] {
					r.ask("%s: reads %s, which contains personal data; confirm purpose and data processing basis", path, t)
				}
			}
			if c.Access == "api" && c.RateLimitRPS > MaxSelfServeRPS {
				r.ask("%s: asks for %d rps, above the self-serve limit of %d", path, c.RateLimitRPS, MaxSelfServeRPS)
			}
		}
	}
}

func checkSchemaChange(r *report, base, status, path, eventType string) {
	switch status {
	case "D":
		r.fail("%s: a published schema version cannot be deleted while producers or consumers may use it; deprecate it instead", path)
	case "M":
		oldRaw, err := git("show", base+":"+path)
		if err != nil {
			r.fail("%s: %v", path, err)
			return
		}
		newRaw, err := os.ReadFile(path)
		if err != nil {
			r.fail("%s: %v", path, err)
			return
		}
		f, err := schema.Compare([]byte(oldRaw), newRaw)
		if err != nil {
			r.fail("%s: %v", path, err)
			return
		}
		for _, b := range f.Breaking {
			r.fail("%s: breaking change, publish it as the next version instead: %s", path, b)
		}
		for _, x := range f.Review {
			r.ask("%s: %s", path, x)
		}
		for _, n := range f.Notes {
			r.notice("%s: %s", path, n)
		}
	case "A":
		raw, err := os.ReadFile(path)
		if err != nil {
			r.fail("%s: %v", path, err)
			return
		}
		existing, _ := git("ls-tree", "--name-only", base, "schemas/"+eventType+"/")
		first := !strings.Contains(existing, ".json")
		reasons, err := schema.ReviewNew(raw, first)
		if err != nil {
			r.fail("%s: %v", path, err)
			return
		}
		for _, x := range reasons {
			r.ask("%s: %s", path, x)
		}
		if first {
			r.notice("%s: new event type %s; latest state and history work with no platform change", path, eventType)
		}
	}
}

func finish(r *report, reviewOut, commentOut string) {
	for _, n := range r.notes {
		fmt.Println("note:   ", n)
	}
	for _, x := range r.review {
		fmt.Println("review: ", x)
	}
	for _, e := range r.errors {
		fmt.Println("error:  ", e)
	}
	var review, comment strings.Builder
	if len(r.review) > 0 {
		review.WriteString("platformcheck asks for cloud-platform review:\n\n- " + strings.Join(r.review, "\n- ") + "\n")
	}
	if len(r.errors) > 0 {
		comment.WriteString("platformcheck found problems that block this PR:\n\n- " + strings.Join(r.errors, "\n- ") + "\n\n")
	}
	comment.WriteString(review.String())
	if len(r.notes) > 0 {
		comment.WriteString("\nNotes for the author and consumers:\n\n- " + strings.Join(r.notes, "\n- ") + "\n")
	}
	for file, body := range map[string]string{reviewOut: review.String(), commentOut: comment.String()} {
		if file == "" || body == "" {
			continue
		}
		if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
			fmt.Println("error:   write", file+":", err)
			os.Exit(1)
		}
	}
	if len(r.errors) > 0 {
		os.Exit(1)
	}
	fmt.Printf("ok: %d notes, %d items for platform review\n", len(r.notes), len(r.review))
}

func git(args ...string) (string, error) {
	out, err := exec.Command("git", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

type discard struct{}

func (discard) Apply(context.Context, store.Event) (store.Outcome, error) {
	return store.Outcome{StateUpdated: true}, nil
}
