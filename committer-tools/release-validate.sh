#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

export MVN_REPO=https://repo1.maven.org/maven2

export CMD_CURL=/usr/bin/curl
export CMD_MD5SUM=/sbin/md5
export CMD_SHASUM=/usr/bin/shasum
export CMD_TAR=/usr/bin/tar
export CMD_UNZIP=/usr/bin/unzip

download_media() {

  echo -n "Downloading ActiveMQ version: $1 type: $2..."
  CURL_DL_OUT=`$CMD_CURL -s -o apache-activemq-$1-bin.$2 $MVN_REPO/org/apache/activemq/apache-activemq/$1/apache-activemq-$1-bin.$2`
  echo "done"

  for kind in sha1 md5
  do 
    echo -n "Downloading ActiveMQ version: $1 type: $2 $kind checksum ..."
    CURL_DL_CHKSUM_OUT=`$CMD_CURL -s -o apache-activemq-$1-bin.$2.$kind $MVN_REPO/org/apache/activemq/apache-activemq/$1/apache-activemq-$1-bin.$2.$kind`
    echo "done"

    echo -n "Validating $kind checksum ..."

    if [ $kind = "sha1" ]; then
      DL_CHKSUM=`$CMD_SHASUM apache-activemq-$1-bin.$2 | cut -f1 -d" "`
    elif [ $kind = "md5" ]; then
      DL_CHKSUM=`$CMD_MD5SUM -q apache-activemq-$1-bin.$2 | cut -f1 -d" "`
    fi
    VL_CHKSUM=`cut -f1 -d" " apache-activemq-$1-bin.$2.$kind`

    if [[ "$DL_CHKSUM" != "$VL_CHKSUM" ]]; then
      echo "ERROR: $kind checksum mismatch expected: $VL_CHKSUM calculated: $DL_CHKSUM"
      exit 1;
    fi
    echo "done"
  done

}

extract_media() {
  echo -n "Extracting $2 media ..."

  if [ $2 = "tar.gz" ]; then
    EXTRACT_OUT=`$CMD_TAR xzf apache-activemq-$1-bin.$2`
  elif [ $2 = "zip" ]; then
    EXTRACT_OUT=`$CMD_UNZIP apache-activemq-$1-bin.$2`
  fi

  if [[ $? -ne 0 ]]; then
    echo "$2 extract failed!"
    exit 1
  fi 
  echo "done"
}

cleanup_media() {
  echo -n "Cleaning up $2 media ...":
  rm apache-activemq-$1-bin.$2.sha1
  rm apache-activemq-$1-bin.$2.md5
  rm apache-activemq-$1-bin.$2
  rm -rf apache-activemq-$1
  echo "done"
}

if [ -z ${1+x} ]; then
  echo "Using default maven repo: $MVN_REPO"
else 
  export MVN_REPO=$1
  echo "Using maven repo: $MVN_REPO"
fi

VERSIONS=()
CURL_OUT=`$CMD_CURL -o index.html -s $MVN_REPO/org/apache/activemq/apache-activemq/`

for i in `grep href index.html | grep -v maven-metadata | grep -v "\.\." | cut -f2 -d= | cut -f1 -d"/" | cut -f2 -d"\"" | sort -rV | grep -v -E '5.1.[0-9+]|5.8'`;
do 
  if [[ $i =~ 5.1[5-9]+ ]]; then
    VERSIONS+=($i)
  fi
done

rm index.html

VERSIONS+=("Quit")

PS3='Install ActiveMQ version: '
select opt in "${VERSIONS[@]}"
do
  case $opt in
    "Quit")
      break;;
  *) 
    for type in tar.gz zip
    do
      download_media $opt $type
      extract_media $opt $type
      cleanup_media $opt $type
    done
    break;;
  esac
done

