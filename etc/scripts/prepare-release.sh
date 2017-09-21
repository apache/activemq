#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Setting the script to fail if anything goes wrong
set -e

#This is a script to help with the release process


error () {
   echo ""
   echo "$@"
   echo ""
   echo "Usage: ./prepare-release.sh repo-url version [target-dir (defaults to version, must not exist)]"
   echo ""
   echo "example:"
   echo "./prepare-release.sh https://repo1.maven.org/maven2 5.15.0"
   echo ""
   exit 64
}

doDownload () {

  repoPath="$1"
  theFile="$2"
  completeURL="$repoPath/$theFile"

  echo
  echo $theFile

  echo "Downloading $completeURL"
  curl $completeURL > $theFile

  echo "Downloading $theFile.asc"
  curl $completeURL.asc > $theFile.asc

  echo "Downloading $theFile.md5"
  curl $completeURL.md5 > $theFile.md5

  echo "Verifying signature $theFile.asc"
  gpg --verify $theFile.asc

  echo "Augmenting $theFile.md5 with filename details"
  echo "  $theFile" >> $theFile.md5

  echo "Generating SHA512 checksum file $theFile.sha512"
  sha512sum $theFile > $theFile.sha512
}

if [ "$#" -lt 2 ]; then
  error "Cannot match arguments"
fi

release=$2
target=${3-$2}
echo "Target Directory: $target"

if [ -d $target ]; then
  error "Directory $target already exists, stopping"
else
  echo "Directory $target does not exist, creating"
  mkdir $target
  cd $target
fi

binRepoURL="$1/org/apache/activemq/apache-activemq/$2"
srcRepoURL="$1/org/apache/activemq/activemq-parent/$2"

doDownload $srcRepoURL activemq-parent-$release-source-release.zip 
doDownload $binRepoURL apache-activemq-$release-bin.zip
doDownload $binRepoURL apache-activemq-$release-bin.tar.gz

echo ""
echo "--- Download Complete for Release $2 Artifacts are in $target---"
echo ""
echo "Validating all MD5 checksum files"
md5sum -c *.md5

echo "Validating all SHA512 checksum files"
sha512sum -c *.sha512


