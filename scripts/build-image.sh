#!/usr/bin/env sh
set -eu

repo_source="https://github.com/dlsrnjs125/rippleguard-governance-service"
revision="$(git rev-parse HEAD)"
tag_revision="$(printf '%s' "${revision}" | cut -c1-12)"
image="rippleguard-governance-service:${tag_revision}"

if [ -n "$(git status --porcelain)" ]; then
  echo "Refusing to build immutable image from a dirty working tree" >&2
  git status --short >&2
  exit 1
fi

./mvnw package

docker build \
  --build-arg "OCI_REVISION=${revision}" \
  --build-arg "OCI_SOURCE=${repo_source}" \
  -t "${image}" \
  .

actual_revision="$(
  docker image inspect "${image}" \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
)"

actual_source="$(
  docker image inspect "${image}" \
    --format '{{ index .Config.Labels "org.opencontainers.image.source" }}'
)"

test "${actual_revision}" = "${revision}"
test "${actual_source}" = "${repo_source}"

printf 'Image: %s\n' "${image}"
printf 'Revision: %s\n' "${actual_revision}"
printf 'Source: %s\n' "${actual_source}"
printf 'Provenance: PASS\n'
