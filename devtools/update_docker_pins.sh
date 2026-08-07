#!/usr/bin/env bash
# Update the pinned ghcr.io/prefix-dev/pixi image tag in the given Dockerfiles
# to the latest published version, and print a Markdown summary of any changes
# to stdout (e.g. for a PR body).
#
# Requires `crane` (google/go-containerregistry) to list the image's tags from
# the public ghcr.io registry -- fetched on demand via `pixi exec` rather than
# added as a project dependency, since it's only ever used here.
set -euo pipefail

IMAGE="ghcr.io/prefix-dev/pixi"

# Bare X.Y.Z tags point at the same image as X.Y.Z-noble (Ubuntu Noble is the
# default variant), so there's no need to pin the "-noble" suffix at all.
new_tag=$(pixi exec --spec crane crane ls "$IMAGE" \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V | tail -n1)

changes=""
for dockerfile in "$@"; do
    current_tag=$(awk -v image="$IMAGE" \
        'NR==1 { sub("^FROM " image ":", ""); print; exit }' "$dockerfile")
    if [[ "$current_tag" == "$new_tag" ]]; then
        echo "${dockerfile}: already at latest (${current_tag})" >&2
        continue
    fi
    tmp=$(mktemp)
    awk -v line="FROM ${IMAGE}:${new_tag}" \
        'NR==1 { print line; next } { print }' "$dockerfile" > "$tmp"
    mv "$tmp" "$dockerfile"
    echo "${dockerfile}: ${current_tag} -> ${new_tag}" >&2
    changes+="* \`${dockerfile}\`: \`${current_tag}\` -> \`${new_tag}\`"$'\n'
done

if [[ -n "$changes" ]]; then
    printf '\n## Docker base image\n\n%s' "$changes"
fi
