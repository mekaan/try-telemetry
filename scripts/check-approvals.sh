#!/usr/bin/env bash
# Passes when every team that owns a changed schema or consumer file (before
# and after the change) has approved the PR's current head commit, plus
# cloud-platform when platformcheck wrote a non-empty REVIEW_FILE.
# Runs from the base branch against a checkout of the PR; see
# .github/workflows/approvals.yml. It reads the PR's files, never runs them.
set -euo pipefail
: "${REPO:?}" "${PR:?}" "${BASE:?}" "${HEAD_SHA:?}"
ORG=${ORG:-acme}

owner_of() { # <ref or empty for working tree> <path>
  local ref=$1 path=$2 dir
  case "$path" in
    consumers/*.yaml)
      if [ -z "$ref" ]; then yq '.owner' "$path"; else git show "$ref:$path" 2>/dev/null | yq '.owner'; fi ;;
    schemas/*)
      dir=$(echo "$path" | cut -d/ -f1-2)
      if [ -z "$ref" ]; then
        cat "$dir"/v*.json 2>/dev/null | jq -rs 'map(."x-owner") | unique | .[]'
      else
        for f in $(git ls-tree --name-only "$ref" "$dir/" | grep -E '/v[0-9]+\.json$'); do
          git show "$ref:$f" | jq -r '."x-owner"'
        done
      fi ;;
  esac
}

required=()
while read -r path; do
  for ref in "" "$BASE"; do
    while read -r owner; do
      [ -n "$owner" ] && [ "$owner" != null ] && required+=("$owner")
    done < <(owner_of "$ref" "$path")
  done
done < <(git diff --name-only "$BASE" HEAD -- schemas consumers)
if [ -s "${REVIEW_FILE:-/nonexistent}" ]; then required+=("@$ORG/cloud-platform"); fi
if [ ${#required[@]} -eq 0 ]; then
  echo "no schema or consumer changes; nothing to approve"
  exit 0
fi

# The latest review per user counts, so a later "changes requested" cancels
# an approval. Only approvals of the current head commit count: pushing new
# commits after approval needs a new approval.
approvers=$(gh api --paginate "repos/$REPO/pulls/$PR/reviews" \
  | jq -rs --arg sha "$HEAD_SHA" 'add | map(select(.state != "COMMENTED")) | group_by(.user.login)[] | last
      | select(.state == "APPROVED" and .commit_id == $sha) | .user.login')

missing=0
for team in $(printf '%s\n' "${required[@]}" | sort -u); do
  slug=${team#@*/}
  ok=false
  for user in $approvers; do
    if [ "$(gh api "orgs/$ORG/teams/$slug/memberships/$user" --jq .state 2>/dev/null)" = active ]; then
      ok=true; break
    fi
  done
  if $ok; then echo "approved by $team"; else echo "missing approval from $team"; missing=1; fi
done
exit $missing
