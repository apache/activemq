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
FROM debian:bookworm-slim AS amq_tpl
ENV DEBIAN_FRONTEND=noninteractive
# activemq_dist can point to a directory or a tarball on the local system
ARG activemq_dist=NOT_SET
# deploy ActiveMQ distribution
ADD $activemq_dist /
RUN mv /apache-activemq-* /apache-activemq
WORKDIR /apache-activemq
RUN apt-get update && apt-get install xmlstarlet -y
RUN xmlstarlet ed -L \
  -N d='http://www.springframework.org/schema/beans' \
  -N a='http://activemq.apache.org/schema/core' \
  -d '//comment()' \
  -d '/d:beans/d:bean[@class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"]/d:property[@name="locations"]' \
  -s '/d:beans/a:broker' -t elem -n plugins \
  -s '/d:beans/a:broker/plugins' -t elem -n simpleAuthenticationPlugin \
  -s '/d:beans/a:broker/plugins/simpleAuthenticationPlugin' -t elem -n users \
  -a '/d:beans/a:broker/plugins/simpleAuthenticationPlugin' -t attr -name anonymousAccessAllowed -v true \
  -s '/d:beans/a:broker/plugins/simpleAuthenticationPlugin/users' -t elem -n authenticationUser \
  -a '/d:beans/a:broker/plugins/simpleAuthenticationPlugin/users/authenticationUser' -t attr -n username -v '${activemq.username}' \
  -a '/d:beans/a:broker/plugins/simpleAuthenticationPlugin/users/authenticationUser' -t attr -name password -v '${activemq.password}' \
  -a '/d:beans/a:broker/plugins/simpleAuthenticationPlugin/users/authenticationUser' -t attr -name groups -v '${activemq.groups}' \
  conf/activemq.xml
RUN rm conf/credentials.properties && touch conf/credentials.properties

FROM eclipse-temurin:17-jre

# ActiveMQ environment variables
ENV ACTIVEMQ_INSTALL_PATH /opt
ENV ACTIVEMQ_HOME $ACTIVEMQ_INSTALL_PATH/apache-activemq
ENV ACTIVEMQ_CONF $ACTIVEMQ_HOME/conf
ENV ACTIVEMQ_OPTS_MEMORY -Xms64M -Xmx1G
ENV ACTIVEMQ_EXEC exec
ENV PATH $PATH:$ACTIVEMQ_HOME/bin
ENV ACTIVEMQ_CONNECTION_USER=system
ENV ACTIVEMQ_CONNECTION_GROUPS=users
ENV ACTIVEMQ_CONNECTION_PASSWORD=manager

# Make the Web console accesible from outside the container
ENV ACTIVEMQ_OPTS='$ACTIVEMQ_OPTS_MEMORY -Djava.util.logging.config.file=logging.properties -Djava.security.auth.login.config=$ACTIVEMQ_CONF/login.config -Djetty.host=0.0.0.0 -Dactivemq.username=$ACTIVEMQ_CONNECTION_USER -Dactivemq.groups=$ACTIVEMQ_CONNECTION_GROUPS -Dactivemq.password=$ACTIVEMQ_CONNECTION_PASSWORD'
#WORKDIR $ACTIVEMQ_HOME

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY --from=amq_tpl /apache-activemq $ACTIVEMQ_INSTALL_PATH/apache-activemq

EXPOSE 8161 61616 5672 61613 1883 61614 1099
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["activemq", "console"]
