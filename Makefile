SHELL := /bin/sh

.PHONY: test package run-local build-image

test:
	./mvnw test

package:
	./mvnw package

run-local:
	./scripts/run-local.sh

build-image:
	./scripts/build-image.sh
