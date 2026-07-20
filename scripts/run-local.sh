#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing .env. Copy .env.example to .env and fill local values." >&2
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

required_variables="
DB_URL
DB_USERNAME
DB_PASSWORD
KAFKA_BOOTSTRAP_SERVERS
"

for variable in $required_variables; do
  eval "value=\${$variable:-}"
  if [ -z "$value" ]; then
    echo "Required environment variable is empty: $variable" >&2
    exit 1
  fi
done

exec "$ROOT_DIR/mvnw" spring-boot:run
