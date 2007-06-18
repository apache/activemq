# ------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------

# ==================================================================
# Helper script to run the IBM JMS performance harness against
# the ActiveMQ message broker.
#
# Sample Usage: 
#   ./perfharness-activemq.sh -d dynamicQueues/FOO -tc jms.r11.PutGet -nt 6
#
# It assumes that the apache-activemq-5.0-SNAPSHOT.jar and 
# perfharness.jar files are in the current directory.  If they are not,
# set the ACTIVEMQ_HOME and PERFHARNESS_HOME env variable to the correct location.
#
# You can download the perfharness.jar file from:
# http://www.alphaworks.ibm.com/tech/perfharness
#
# By Default the test connects the the vm://localhost broker.  To change that, use
# set the BROKER_URL to the broker url you want to use.
#
# ==================================================================

if [ -z "$PERFHARNESS_HOME" ] ; then
   PERFHARNESS_HOME=.
fi

if [ -z "$ACTIVEMQ_HOME" ] ; then
   ACTIVEMQ_HOME=../..
fi

if [ -z "$BROKER_URL" ] ; then
   BROKER_URL='vm://(broker://()/localhost?useJmx=false)/localhost'
fi

java ${JAVA_OPTIONS} -cp ${ACTIVEMQ_HOME}/apache-activemq-5.0-SNAPSHOT.jar:${PERFHARNESS_HOME}/perfharness.jar JMSPerfHarness -pc JNDI -ii org.apache.activemq.jndi.ActiveMQInitialContextFactory -iu $BROKER_URL -cf ConnectionFactory -d dynamic$DESTINATION $@
