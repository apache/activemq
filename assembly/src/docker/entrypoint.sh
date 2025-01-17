#!/bin/bash -x

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

# JMX security
if [ -n "$JMX_USER" -o -n"$ACTIVEMQ_JMX_USER" ]; then
  ACTIVEMQ_SUNJMX_START="-Dcom.sun.management.jmxremote.authenticate=true"
  ACTIVEMQ_SUNJMX_START="$ACTIVEMQ_SUNJMX_START -Dcom.sun.management.jmxremote.password.file=${ACTIVEMQ_HOME}/conf/jmx.password"
  ACTIVEMQ_SUNJMX_START="$ACTIVEMQ_SUNJMX_START -Dcom.sun.management.jmxremote.access.file=${ACTIVEMQ_HOME}/conf/jmx.access"
  export ACTIVEMQ_SUNJMX_START
fi

if [ -n "${ACTIVEMQ_JMX_USER}" ]; then
  if [ -f "${ACTIVEMQ_HOME}/conf/jmx.security.enabled" ]; then
    echo "JMX Security already enabled"
  else
     echo "Enabling ActiveMQ JMX security"
     sed -i "s/admin/${ACTIVEMQ_JMX_USER}/" ${ACTIVEMQ_HOME}/conf/jmx.access
     sed -i "s/admin/${ACTIVEMQ_JMX_USER}/" ${ACTIVEMQ_HOME}/conf/jmx.password
     if [ -n "${ACTIVEMQ_JMX_PASSWORD}" ]; then
       sed -i "s/\ activemq/\ ${ACTIVEMQ_JMX_PASSWORD}/" ${ACTIVEMQ_HOME}/conf/jmx.password
     fi
     touch "${ACTIVEMQ_HOME}/conf/jmx.security.enabled"
  fi
fi

# WebConsole security
if [ -n "${ACTIVEMQ_WEB_USER}" ]; then
  echo "Enabling ActiveMQ WebConsole security"
  sed -i s/$ACTIVEMQ_WEB_DEFAULT_USER=/${ACTIVEMQ_WEB_USER}=/g ${ACTIVEMQ_HOME}/conf/users.properties
  if [ -n "${ACTIVEMQ_WEB_PASSWORD}" ]; then
    sed -i s/=$ACTIVEMQ_WEB_DEFAULT_PASSWORD/=${ACTIVEMQ_WEB_PASSWORD}/g ${ACTIVEMQ_HOME}/conf/users.properties
  fi
  # ACTIVEMQ 5.x
  if [ -f "${ACTIVEMQ_HOME}/conf/jetty-realm.properties" ]; then
    sed -i "s/${ACTIVEMQ_WEB_DEFAULT_USER}: /${ACTIVEMQ_WEB_DEFAULT_USER}: /" ${ACTIVEMQ_HOME}/conf/jetty-realm.properties
    if [ -n "${ACTIVEMQ_WEB_PASSWORD}" ]; then
      sed -i "s/${ACTIVEMQ_WEB_DEFAULT_USER}: ${ACTIVEMQ_WEB_DEFAULT_PASSWORD}/${ACTIVEMQ_WEB_USER}: ${ACTIVEMQ_WEB_PASSWORD}/" ${ACTIVEMQ_HOME}/conf/jetty-realm.properties
    fi
  fi
fi

exec "$@"
