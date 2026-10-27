---
title: "zOS"
slug: /zos
---


Running ActiveMQ on z/OS
========================

It is relatively straightforward to run the ActiveMQ broker on z/OS.

There are a couple of steps:

1\. Copy ActiveMQ to z/OS  
2\. Modify the configuration  
3\. Run using JZOS  
4\. Test

### Copy ActiveMQ to z/OS

*   Ensure that the 1.5 JVM is available on z/OS, together with the [JZOS](http://www.dovetail.com/docs/jzos/index.html) component.
*   Download the .zip version of ActiveMQ from Apache.
*   FTP the broker to a suitable directory in USS.
*   Log into USS using rlogin or whatever has been configured at your site to do a login into USS.
*   Extract the zip file using the command: jar -xvf apache-activemq-5.0.0.zip
*   This will extract the whole package in ASCII, so do not except any of the files to be viewable on z/OS, except using special editors suitable for ASCII.
*   Maybe rename the directory to which the files were extracted to something shorter or create a softlink for more convenient access.

### Modify the configuration

Currently, the Jetty plugin does not work on z/OS, so need to be disabled in the activemq.xml file.

*   FTP the activemq.xml file from the conf directory in the ActiveMQ installation to your PC in binary mode.
*   Edit the XML file using your XML editor or a text editor like Wordpad.
*   Search for "jetty" in the XML.
*   Change the comment block so that jetty is completely commented out.
*   Save the file.
*   FTP the file back into the location on z/OS it came from, again in binary mode, so that it is preserved as an ASCII file.

### Run using JZOS

I have renamed the lib/optional directory in USS and shortened some of the lib/optional jar names to make the JCL easier to handle. The JCL to run the ActiveMQ broker is then:

`ACTIVEMQ.JCL**

//ACTIVEMQ JOB   (),
//         CLASS=A,                                                    
//         MSGCLASS=X,                                                 
//         MSGLEVEL=(1,1),                                             
//         NOTIFY=&SYSUID,                                             
//         REGION=0M,                                                  
//         TIME=1440                                                   
//PROCLIB JCLLIB ORDER=<JZOS.SYS1.PROCLIB>
//\*                                                                    
//\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*
//\*                                                                    
//\* Batch job to run Tomcat under JZOS                                 
//\*                                                                    
//\* Tailor the proc and job for your installation:                     
//\* 1.) Modify the Job card per your installation's requirements       
//\* 2.) Modify the PROCLIB card to point to JZOS proclib               
//\* 3.) Set VERSION='14' for Java 1.4 or VERSION='50' for Java 5       
//\* 4.) Set JAVA_HOME to point the location of the Java SDK            
//\* 5.) Set CATALINA_HOME to point to the shared Tomcat install dir    
//\* 6.) (Optional) set CATALINA_BASE to point to instance specific     
//\*     Tomcat base dir                                                
//\*                                                                    
//\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*
//\*                                                                    
//JAVA EXEC PROC=JVMPRC50,                                             
// LOGLVL='+D',
// JAVACLS='org.apache.activemq.console.Main',
// ARGS='start'
//STDENV DD *
\# This is a shell script which configures
\# any environment variables for the Java JVM.
\# Variables must be exported to be seen by the launcher.
. /etc/profile
export JAVA\_HOME=/space/javaV5\_31/J5.0 
AMQ_HOME=/home/user/activemq/amq
ACTIVEMQ\_BASE="$AMQ\_HOME"

export PATH=/bin:"${JAVA_HOME}"/bin:

LIBPATH=/lib:/usr/lib:"${JAVA_HOME}"/bin
LIBPATH="$LIBPATH":"${JAVA_HOME}"/bin/classic
export LIBPATH="$LIBPATH":

CLASSPATH="${JAVA_HOME}/lib/tools.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/conf"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/activemq-all-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/bin/run.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/bin/wrapper.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activation-1.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activemq-console-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activemq-core-5.0.0-tests.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activemq-core-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activemq-jaas-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/activemq-web-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/camel-core-1.2.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/camel-jms-1.2.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/camel-spring-1.2.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/commons-logging-1.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ\_HOME/lib/geronimo-j2ee-management\_1.0_spec-1.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ\_HOME/lib/geronimo-jms\_1.1_spec-1.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ\_HOME/lib/geronimo-jta\_1.0.1B_spec-1.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/jaxb-api-2.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/jaxb-impl-2.0.3.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/stax-1.2.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/stax-api-1.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/spring-2.0.6.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/xbean.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/jetty-6.1.4.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/jetty-util-6.1.4.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/activeio-core-3.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/geronimo-j2ee-connector.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/activeio-core-tests.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/geronimo-j2ee-jacc.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/activemq-optional-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/activemq-xmpp-5.0.0.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/jmdns-1.0-RC2.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/commons-beanutils-1.6.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/log4j-1.2.14.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/commons-collections-3.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/servlet-api-2.5-6.1.4.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/commons-dbcp-1.2.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/commons-httpclient-2.0.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/commons-pool-1.2.jar"
CLASSPATH="$CLASSPATH":"$AMQ\_HOME/lib/opt/xmlpull-1.1.3.4d\_b4_min.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/derby-10.1.3.1.jar"
CLASSPATH="$CLASSPATH":"$AMQ_HOME/lib/opt/xstream-1.1.2.jar"
export CLASSPATH="$CLASSPATH":

\# Set JZOS specific options
\# Use this variable to specify encoding for DD STDOUT and STDERR
#export JZOS\_OUTPUT\_ENCODING=IBM-1047
\# Use this variable to prevent JZOS from handling MVS operator commands
#export JZOS\_ENABLE\_MVS_COMMANDS=false
\# Use this variable to supply additional arguments to main
#export JZOS\_MAIN\_ARGS=""

\# Configure JVM options
\# Note that ActiveMQ requires default ASCII file.encoding
IJO="-Xms64m -Xmx512m"
IJO="$IJO -Dfile.encoding=ISO8859-1"
IJO="$IJO -Dcom.sun.management.jmxremote"
IJO="$IJO -Dorg.apache.activemq.UseDedicatedTaskRunner=true"
IJO="$IJO -Dderby.system.home=${ACTIVEMQ_BASE}/data"
IJO="$IJO -Dderby.storage.fileSyncTransactionLog=true"
IJO="$IJO -Djavax.net.ssl.keyStorePassword=password"
IJO="$IJO -Djavax.net.ssl.trustStorePassword=password"
IJO="$IJO -Djavax.net.ssl.keyStore=${ACTIVEMQ_BASE}/conf/broker.ks"
IJO="$IJO -Djavax.net.ssl.trustStore=${ACTIVEMQ_BASE}/conf/broker.ts"
IJO="$IJO -Dactivemq.classpath=${CLASSPATH}"
IJO="$IJO -Dactivemq.base=${ACTIVEMQ_BASE}"
IJO="$IJO -Dactivemq.home=${AMQ_HOME}"
IJO="$IJO -Djava.io.tmpdir=${ACTIVEMQ_BASE}/temp"

\# Configure SDK5.0 to use shared classes (at group level)
\# You must comment this out if you are not running SDK 5
groupname=\`id -gn\`
IJO="$IJO -Xshareclasses:name=$groupname,groupAccess"
export IBM\_JAVA\_OPTIONS="$IJO "

export JAVA\_DUMP\_HEAP=false
export JAVA_PROPAGATE=NO
export IBM\_JAVA\_ZOS_TDUMP=NO
//

### Test

Once the broker has been started on z/OS, modify one of the example application to have the hostname of the z/OS system and run it to confirm that the broker is functioning correctly.

You can also gather information about the broker as usual using [jmx](jmx).

