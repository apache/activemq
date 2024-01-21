#!/bin/bash

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

# Transport/connection security
if [ -n "${ACTIVEMQ_CONNECTION_USER}" ]; then
  if [ -f "${ACTIVEMQ_HOME}/conf/connection.security.enabled" ]; then
    echo "ActiveMQ Connection Security enabled"
  else
    echo "Enabling ActiveMQ Connection Security"
    sed -i "s/activemq.username=system/activemq.username=${ACTIVEMQ_CONNECTION_USER}/" ${ACTIVEMQ_HOME}/conf/credentials.properties
    sed -i "s/activemq.password=manager/activemq.password=${ACTIVEMQ_CONNECTION_PASSWORD}/" ${ACTIVEMQ_HOME}/conf/credentials.properties
    read -r -d '' REPLACE << END
      <plugins>
        <simpleAuthenticationPlugin>
          <users>
            <authenticationUser username="$\{activemq.username}" password="$\{activemq.password}"/>
          </users>
        </simpleAuthenticationPlugin>
      </plugins>
    </broker>
END
    REPLACE=${REPLACE//$\\/$}
    REPLACE=${REPLACE//\//\\\/}
    REPLACE=$(echo $REPLACE | tr '\n' ' ')
    sed -i "s/<\/broker>/$REPLACE/" ${ACTIVEMQ_HOME}/conf/activemq.xml
    touch "${ACTIVEMQ_HOME}/conf/connection.security.enabled"
  fi
fi

# JMX security
if [ -n "${ACTIVEMQ_JMX_USER}" ]; then
  if [ -f "${ACTIVEMQ_HOME}/conf/jmx.security.enabled" ]; then
    echo "JMX Security already enabled"
  else
     echo "Enabling ActiveMQ JMX security"
     read -r -d '' REPLACE << END
       <managementContext>
         <managementContext createConnector="true" />
       </managementContext>
     </broker>
END
     REPLACE=${REPLACE//\//\\\/}
     REPLACE=${REPLACE//$\\/$}
     REPLACE=$(echo $REPLACE | tr '\n' ' ')
     sed -i "s/<\/broker>/$REPLACE/" ${ACTIVEMQ_HOME}/conf/activemq.xml
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
  sed -i s/admin=/${ACTIVEMQ_WEB_USER}=/g ${ACTIVEMQ_HOME}/conf/users.properties
  if [ -n "${ACTIVEMQ_WEB_PASSWORD}" ]; then
    sed -i s/=admin/=${ACTIVEMQ_WEB_PASSWORD}/g ${ACTIVEMQ_HOME}/conf/users.properties
  fi
fi

exec "$@"
