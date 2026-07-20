SHELL := /bin/sh

IMAGE_NAME := rippleguard-governance-service
OCI_SOURCE := https://github.com/dlsrnjs125/rippleguard-governance-service
OCI_REVISION := $(shell git rev-parse HEAD)
IMAGE_TAG := $(shell git rev-parse --short=12 HEAD)
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: test package run-local docker-build verify-image-labels build-image require-clean require-env

test:
	./mvnw test

package:
	./mvnw package

require-clean:
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Refusing to build immutable image from a dirty working tree" >&2; \
		git status --short >&2; \
		exit 1; \
	fi

require-env:
	@if [ ! -f .env ]; then \
		echo "Missing .env. Copy .env.example to .env and fill local values." >&2; \
		exit 1; \
	fi
	@set -a; . ./.env; set +a; \
	for variable in DB_URL DB_USERNAME DB_PASSWORD KAFKA_BOOTSTRAP_SERVERS; do \
		eval "value=\$${$${variable}:-}"; \
		if [ -z "$${value}" ]; then \
			echo "Required environment variable is empty: $${variable}" >&2; \
			exit 1; \
		fi; \
	done

run-local: require-env
	set -a; . ./.env; set +a; ./mvnw spring-boot:run

docker-build: package require-clean
	docker build \
		--build-arg "OCI_REVISION=$(OCI_REVISION)" \
		--build-arg "OCI_SOURCE=$(OCI_SOURCE)" \
		-t "$(IMAGE)" \
		.

verify-image-labels: docker-build
	@actual_revision="$$(docker image inspect "$(IMAGE)" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"; \
	actual_source="$$(docker image inspect "$(IMAGE)" --format '{{ index .Config.Labels "org.opencontainers.image.source" }}')"; \
	test "$${actual_revision}" = "$(OCI_REVISION)"; \
	test "$${actual_source}" = "$(OCI_SOURCE)"; \
	printf 'Image: %s\n' "$(IMAGE)"; \
	printf 'Revision: %s\n' "$${actual_revision}"; \
	printf 'Source: %s\n' "$${actual_source}"; \
	printf 'Provenance: PASS\n'

build-image: verify-image-labels
