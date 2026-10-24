#!/bin/sh
#
# Copyright 2021-2022 Alibaba Group Holding Ltd.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

set -eu
umask 077

if [ "$#" -ne 1 ]; then
    printf 'Usage: %s /absolute/path/outside-the-checkout/example.p12\n' "$0" >&2
    exit 1
fi
case "$1" in
    /*) ;;
    *) printf 'The keystore path must be absolute.\n' >&2; exit 1 ;;
esac
if [ -e "$1" ] || [ -L "$1" ]; then
    printf 'Refusing to overwrite an existing keystore.\n' >&2
    exit 1
fi
destination_dir=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd -P)
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
checkout=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || true)
if [ -n "$checkout" ]; then
    case "$destination_dir/" in
        "$checkout/"*) printf 'Keep the keystore outside the source checkout.\n' >&2; exit 1 ;;
    esac
fi

# keytool prompts for the password; it is never embedded in this script or its arguments.
# Use the same password in FML_HTTPS_KEYSTORE_PASSWORD and this path in FML_HTTPS_KEYSTORE_PATH.
keytool -genkeypair -keyalg RSA -keysize 2048 -alias self_signed -storetype PKCS12 -keystore "$1"
