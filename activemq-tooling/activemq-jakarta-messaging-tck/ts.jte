#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

###############################################################################
# Jakarta Messaging TCK 3.1.0 - Apache ActiveMQ configuration
#
# jms.home and jms.classes are appended dynamically by run_tck.sh
###############################################################################

# ---- Classpaths (harness, compile, runtime) --------------------------------
ts.harness.classpath=${ts.home}/lib/javatest.jar${pathsep}${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/jmstck.jar${pathsep}${ant.home}/lib/ant.jar
ts.classpath=${jms.classes}${pathsep}${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/jmstck.jar
ts.run.classpath=${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/sigtest.jar${pathsep}${ts.home}/lib/jmstck.jar${pathsep}${ts.home}/classes${pathsep}${ts.home}/bin${pathsep}${jms.classes}

# ---- Signature test settings -----------------------------------------------
jimage.dir=${ts.home}/tmp/jdk-bundles
sigTestClasspath=${jms.classes}${pathsep}${jimage.dir}/java.base${pathsep}${jimage.dir}/java.rmi${pathsep}${jimage.dir}/java.sql${pathsep}${jimage.dir}/java.naming

# ---- Harness settings ------------------------------------------------------
harness.temp.directory=${ts.home}/tmp
harness.log.port=2001
harness.log.traceflag=true
harness.log.delayseconds=1
harness.executeMode=2
harness.socket.retry.count=10
work.dir=/tmp/JTwork
report.dir=/tmp/JTreport
if.existing.work.report.dirs=auto

# ---- Timezone --------------------------------------------------------------
tz=UTC

# ---- Timeout factor --------------------------------------------------------
javatest.timeout.factor=2

# ---- Build level (1=compile only) ------------------------------------------
build.level=1

# ---- Display (Unix) --------------------------------------------------------
ts.display=:0.0

# ---- Deliverable class -----------------------------------------------------
deliverable.class=com.sun.ts.lib.deliverable.jms.JMSDeliverable

# ---- Test execution: run tests in same JVM as harness ----------------------
# This avoids forking a process with -Djava.security.manager which is
# unsupported on Java 17+.
command.testExecuteSameJVM=com.sun.ts.lib.harness.ExecuteTSTestSameJVMCmd \
        $testExecuteClass $testExecuteArgs

# ---- Test execution: forked JVM (no security manager) ----------------------
command.testExecute=com.sun.ts.lib.harness.ExecTSTestCmd \
        CLASSPATH=${ts.run.classpath} \
        DISPLAY="${ts.display}" \
        HOME="${user.home}" \
        windir=${windir} \
        SYSTEMROOT=${SYSTEMROOT} \
        ${JAVA_HOME}/bin/java \
        -Djava.naming.factory.initial=org.apache.activemq.tck.JNDIInitialContextFactory \
        -Ddeliverable.class=${deliverable.class} \
        $testExecuteClass $testExecuteArgs

env.ts_unix.menu=true
env.ts_win32.menu=true

# ---- JMS porting class (uses JNDI to look up administered objects) ---------
porting.ts.jmsObjects.class.1=com.sun.ts.lib.implementation.sun.jms.SunRIJMSObjects

# ---- JNDI configuration ---------------------------------------------------
java.naming.factory.initial=org.apache.activemq.tck.JNDIInitialContextFactory

# ---- JMS TCK settings -----------------------------------------------------
jms_timeout=10000
user=guest
password=guest

# ---- Platform mode (standalone = no Jakarta EE container) ------------------
platform.mode=standalone

# ---- TCK library name -----------------------------------------------------
tslib.name=jmstck

# ---- Implementation under test (use 'ri' porting layer) -------------------
impl.vi=ri

# ---- RI / vendor configuration (paths not used for ActiveMQ) --------------
ri.home=${jms.home}
ri.jars=${jms.classes}
admin.user=admin
admin.pass=admin
admin.pass.file=/tmp/ripassword
jndi.fs.dir=/tmp/ri_admin_objects
jndi.factory.initial="java.naming.factory.initial=org.apache.activemq.tck.JNDIInitialContextFactory"
jndi.provider.url="java.naming.provider.url=file:///${jndi.fs.dir}"

# jms.home and jms.classes are appended dynamically by run_tck.sh
