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

FROM eclipse-temurin:17-jre

# ActiveMQ environment variables
ENV ACTIVEMQ_INSTALL_PATH /opt
ENV ACTIVEMQ_HOME $ACTIVEMQ_INSTALL_PATH/apache-activemq
ENV ACTIVEMQ_CONF $ACTIVEMQ_HOME/conf
ENV ACTIVEMQ_OPTS_MEMORY -Xms64M -Xmx1G
ENV ACTIVEMQ_EXEC exec
ENV PATH $PATH:$ACTIVEMQ_HOME/bin
# Make the Web console accesible from outside the container
ENV ACTIVEMQ_OPTS $ACTIVEMQ_OPTS_MEMORY -Djava.util.logging.config.file=logging.properties -Djava.security.auth.login.config=$ACTIVEMQ_CONF/login.config -Djetty.host=0.0.0.0
#WORKDIR $ACTIVEMQ_HOME

ENV ACTIVEMQ_ADMIN_USER=admin
ENV ACTIVEMQ_ADMIN_PASSWORD=admin

# activemq_dist can point to a directory or a tarball on the local system
ARG activemq_dist=NOT_SET

COPY src/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Install build dependencies and activemq
ADD $activemq_dist $ACTIVEMQ_INSTALL_PATH
RUN set -x && \
  cp -r $ACTIVEMQ_INSTALL_PATH/apache-activemq-* $ACTIVEMQ_HOME && \
	rm -r $ACTIVEMQ_INSTALL_PATH/apache-activemq-*

EXPOSE 8161 61616 5672 61613 1883 61614 1099

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD curl -sf \
    -u "${ACTIVEMQ_ADMIN_USER}:${ACTIVEMQ_ADMIN_PASSWORD}" \
    -H "Origin: http://127.0.0.1" \
    "http://127.0.0.1:8161/api/jolokia/search/org.apache.activemq:type=Broker,*" \
    >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["activemq", "console"]
