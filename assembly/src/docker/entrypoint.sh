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

# Generate the configuration files according to the environment variables related
# to the security that have been set
configure_security() {
  if [ -f "${ACTIVEMQ_HOME}/conf/activemq-security.xml" ]; then
      echo "Security settings already configured"
  else
    echo "Configuring security settings"
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
    REPLACE=${REPLACE//\//\\\/}
    REPLACE=${REPLACE//$\\/$}
    REPLACE=$(echo $REPLACE | tr '\n' ' ')
    sed "s/<\/broker>/$REPLACE/" ${ACTIVEMQ_HOME}/conf/activemq.xml > ${ACTIVEMQ_HOME}/conf/activemq-security.xml
    sed -i "s/credentials.properties/credentials-security.properties/" ${ACTIVEMQ_HOME}/conf/activemq-security.xml

    cat > ${ACTIVEMQ_HOME}/conf/credentials-security.properties <<- END
activemq.username=${ACTIVEMQ_USER}
activemq.password=${ACTIVEMQ_PASSWORD}
END
  fi
}
if [[ $# -eq 2 ]] && [[ "$1" -eq activemq ]] && [[ "$2" -eq console ]] && [[ -n "${ACTIVEMQ_USER}" ]]; then
  configure_security
  activemq console xbean:activemq-security.xml
else
  exec "$@"
fi
