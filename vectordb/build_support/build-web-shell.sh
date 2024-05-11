#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/.."

vdbms_PRIVATE_VERSION="$(git -C "${DIR}/../.." describe --always --dirty)"
vdbms_PUBLIC_VERSION="$(git -C "${DIR}/../../vdbms_public" describe --always --dirty)"
vdbms_BUILD_TIME="$(date +%Y%m%d)"

vdbms_SHELL_DIRECTORY="vdbms"
vdbms_BPT_DIRECTORY="bpt-printer"

mkdir -p cmake-build-wasm
cd cmake-build-wasm
emcmake cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel

rm -rf deploy
mkdir -p deploy
cp ../.gitignore deploy
cp ../LICENSE deploy
cp ../tools/wasm-shell/extra_files/vercel.json deploy

make -j$(nproc) wasm-shell
mkdir -p "deploy/${vdbms_SHELL_DIRECTORY}"
cp bin/vdbms-wasm-shell.js "deploy/${vdbms_SHELL_DIRECTORY}"
cp bin/vdbms-wasm-shell.wasm "deploy/${vdbms_SHELL_DIRECTORY}"
cp -a ../tools/wasm-shell/extra_files/index.html "deploy/${vdbms_SHELL_DIRECTORY}"
cp ../logo/vdbms.svg "deploy/${vdbms_SHELL_DIRECTORY}"
sed -i '' "s|\${vdbms_PRIVATE_VERSION}|${vdbms_PRIVATE_VERSION}|" "deploy/${vdbms_SHELL_DIRECTORY}/index.html"
sed -i '' "s|\${vdbms_PUBLIC_VERSION}|${vdbms_PUBLIC_VERSION}|" "deploy/${vdbms_SHELL_DIRECTORY}/index.html"
sed -i '' "s|\${vdbms_BUILD_TIME}|${vdbms_BUILD_TIME}|" "deploy/${vdbms_SHELL_DIRECTORY}/index.html"

make -j$(nproc) wasm-bpt-printer
mkdir -p "deploy/${vdbms_BPT_DIRECTORY}"
cp bin/vdbms-wasm-bpt-printer.js "deploy/${vdbms_BPT_DIRECTORY}"
cp bin/vdbms-wasm-bpt-printer.wasm "deploy/${vdbms_BPT_DIRECTORY}"
cp -a ../tools/wasm-bpt-printer/extra_files/index.html "deploy/${vdbms_BPT_DIRECTORY}"
cp ../logo/vdbms.svg "deploy/${vdbms_BPT_DIRECTORY}"
sed -i '' "s|\${vdbms_PRIVATE_VERSION}|${vdbms_PRIVATE_VERSION}|" "deploy/${vdbms_BPT_DIRECTORY}/index.html"
sed -i '' "s|\${vdbms_PUBLIC_VERSION}|${vdbms_PUBLIC_VERSION}|" "deploy/${vdbms_BPT_DIRECTORY}/index.html"
sed -i '' "s|\${vdbms_BUILD_TIME}|${vdbms_BUILD_TIME}|" "deploy/${vdbms_BPT_DIRECTORY}/index.html"

ls -alh "deploy/${vdbms_SHELL_DIRECTORY}"
ls -alh "deploy/${vdbms_BPT_DIRECTORY}"
