#!/bin/bash

set -e

BASE_DIR=/opt/mcp-server
IMAGE_NAME=mcp-ingestion:latest
ENV_FILE=$BASE_DIR/.env
LOG_DIR=/var/log/mcp-ingestion

mkdir -p $LOG_DIR

/usr/bin/docker run --rm \
  --env-file $ENV_FILE \
  $IMAGE_NAME \
  >> $LOG_DIR/cron.log 2>&1

