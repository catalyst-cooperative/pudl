#!/usr/bin/env bash
# Re-pin every skill in skills-lock.json to its source repo's latest available ref, and
# print a Markdown summary of any changes to stdout (e.g. for a PR body).
#
# Requires `gh` (authenticated) to query GitHub for tags/commits, and `npx` (from the
# `nodejs` pixi dependency) to run the `skills` CLI.
set -euo pipefail

LOCKFILE="skills-lock.json"

# Print every tag in the repo named by $1, one per line, oldest API page first.
all_tags() {
    gh api --paginate "repos/$1/tags" --jq '.[].name'
}

latest_ref() {
    local name="$1" source="$2"
    local all
    all=$(all_tags "$source")

    local prefixed
    prefixed=$(echo "$all" | grep -E "^${name}-" || true)
    if [[ -n "$prefixed" ]]; then
        # Strip the "<name>-" prefix so `sort -V` compares versions, not skill names.
        echo "${name}-$(echo "$prefixed" | sed "s/^${name}-//" | sort -V | tail -n1)"
        return
    fi

    if [[ -n "$all" ]]; then
        echo "$all" | sort -V | tail -n1
        return
    fi

    local branch
    branch=$(gh api "repos/${source}" --jq '.default_branch')
    gh api "repos/${source}/commits/${branch}" --jq '.sha'
}

changes=""
# `npx skills add` reads from stdin internally so it cannot run inside a `while read ...
# done < <(...)` loop: it silently steals lines meant for the loop's own `read`, causing
# later skills in the list to be skipped. Slurping the list into an array first, then
# iterating with a plain `for`, keeps stdin untouched.
mapfile -t skill_lines < <(python3 -c "
import json
with open('${LOCKFILE}') as f:
    data = json.load(f)
for name, entry in data['skills'].items():
    print(f\"{name}\t{entry['source']}\t{entry.get('ref', '')}\")
")

for line in "${skill_lines[@]}"; do
    IFS=$'\t' read -r name source current_ref <<<"$line"
    new_ref=$(latest_ref "$name" "$source")
    if [[ "$new_ref" == "$current_ref" ]]; then
        echo "${name}: already at latest (${current_ref})" >&2
        continue
    fi
    npx --yes skills@latest add "${source}#${new_ref}@${name}" -y >&2
    echo "${name}: ${current_ref} -> ${new_ref}" >&2
    changes+="* \`${name}\` (\`${source}\`): \`${current_ref}\` -> \`${new_ref}\`"$'\n'
done

if [[ -n "$changes" ]]; then
    printf '\n## Agent skills\n\n%s' "$changes"
fi
