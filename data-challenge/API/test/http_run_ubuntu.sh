#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SOURCE_FILE="${DIR/test/CF}/main.py"

echo "$DIR"
echo "$SOURCE_FILE"

source "${DIR}/.env.local"

functions-framework \
  --source=${SOURCE_FILE} \
  --target=${FUNCTION_NAME_HTTP} \
  --signature-type=http \
  --port=${FUNCTION_PORT_HTTP} \
  --debug