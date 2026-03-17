#!/usr/bin/env bash
set -euo pipefail

if [ ! -f .env ]; then
  cp .env.example .env
fi

mkdir -p logs

make up
make create-topics
nohup make bronze-silver > logs/bronze-silver.log 2>&1 &
sleep 8
make replay || true
sleep 8
make gold
