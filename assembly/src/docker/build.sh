#!/bin/sh

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

usage() {
  cat <<HERE
Usage:
  build.sh --from-local-dist [--archive <archive>] [--image-name <image>] [--build-multi-platform <comma-separated platforms>]
  build.sh --from-release --activemq-version <x.x.x> [--image-name <image>] [--build-multi-platform <comma-separated platforms>]
  build.sh --help

  If the --image-name flag is not used the built image name will be 'activemq'.
  Check the supported build platforms; you can verify with this command: docker buildx ls
  The supported platforms (OS/Arch) depend on the build's base image, in this case [eclipse-temurin:11-jre](https://hub.docker.com/_/eclipse-temurin).

HERE
  exit 1
}

while [ $# -ge 1 ]
do
key="$1"
  case $key in
    --from-local-dist)
    FROM_LOCAL="true"
    ;;
    --from-release)
    FROM_RELEASE="true"
    ;;
    --image-name)
    IMAGE_NAME="$2"
    shift
    ;;
    --archive)
    ARCHIVE="$2"
    shift
    ;;
    --activemq-version)
    ACTIVEMQ_VERSION="$2"
    shift
    ;;
    --build-multi-platform)
    BUILD_MULTI_PLATFORM="$2"
    shift
    ;;
    --help)
    usage
    ;;
    *)
    # unknown option
    ;;
  esac
  shift
done

IMAGE_NAME=${IMAGE_NAME:-activemq}

# TMPDIR must be contained within the working directory so it is part of the
# Docker context. (i.e. it can't be mktemp'd in /tmp)
TMPDIR=_TMP_

cleanup() {
    rm -rf "${TMPDIR}"
}
trap cleanup EXIT

mkdir -p "${TMPDIR}"

if [ -n "${FROM_RELEASE}" ]; then

  [ -n "${ACTIVEMQ_VERSION}" ] || usage

  ACTIVEMQ_BASE_URL="https://dlcdn.apache.org/activemq/${ACTIVEMQ_VERSION}/"
  ACTIVEMQ_DIST_FILE_NAME="apache-activemq-${ACTIVEMQ_VERSION}-bin.tar.gz"
  CURL_OUTPUT="${TMPDIR}/${ACTIVEMQ_DIST_FILE_NAME}"

  echo "Downloading ${ACTIVEMQ_DIST_FILE_NAME} from ${ACTIVEMQ_BASE_URL}"
  curl -s "${ACTIVEMQ_BASE_URL}${ACTIVEMQ_DIST_FILE_NAME}" --output "${CURL_OUTPUT}"

  ACTIVEMQ_DIST="${CURL_OUTPUT}"

elif [ -n "${FROM_LOCAL}" ]; then

  if [ -n "${ARCHIVE}" ]; then
     DIST_DIR=${ARCHIVE}
  else
     DIST_DIR="target/apache-activemq-*.tar.gz"
  fi
  ACTIVEMQ_DIST=${TMPDIR}/apache-activemq.tar.gz
  echo "Using ActiveMQ dist: ${DIST_DIR}"
  cp ${DIST_DIR} ${ACTIVEMQ_DIST}

else

  usage

fi

if [ -n "${BUILD_MULTI_PLATFORM}" ]; then
  echo "Checking if buildx installed..."
  VERSION_BUILD_X=$(docker buildx version) > /dev/null 2>&1

  if [ $? -eq 0 ]; then
    echo "Found buildx {${VERSION_BUILD_X}} on your docker system"
    echo "Starting build of the docker image for the platform ${BUILD_MULTI_PLATFORM}"

    BUILD_X="buildx"
    BUILD_X_FLAG="--push"
    BUILD_X_PLATFORM="--platform ${BUILD_MULTI_PLATFORM}"
  else
    echo "Error: buildx not installed with your docker system"
    exit 2
  fi

fi

docker ${BUILD_X} build ${BUILD_X_PLATFORM} --build-arg activemq_dist="${ACTIVEMQ_DIST}" ${BUILD_X_FLAG} -t "${IMAGE_NAME}" .
