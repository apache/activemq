Deploying the ActiveMQ-WebConsole
=================================

In the default configuration ActiveMQ automatically starts the web console in the
same VM as the broker. The console is accessibly under http://localhost:8161/admin/.


However it's also possible to start the web console in a seperate VM and connect it
to the broker via JMS and JMX. The reasons to do so may include increased reliablity
of the broker itself (f.e. the embedded web console could use up all the available
memory) or the monitoring of a master/slave system.

Just deploy the war into your prefered servlet container and add the apache-activemq.jar
to the classpath of the container (f.e. under Tomcat that'd be common/lib and under
Jetty the lib-directory). Two options are available for the configuration of the broker
and jmx uri(s):

 * System Properties
   -----------------
     Specify the following system properties in your webcontainer:
         -Dwebconsole.type=properties
         -Dwebconsole.jms.url=<url of the broker> (f.e. tcp://localhost:61616)
         -Dwebconsole.jmx.url=<jmx url to the broker> (f.e. service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi)
 
 * JNDI
   ----
     If your servlet container supports JNDI it's possible to use a JMS-ConnectionFactory
     configured outside the war:
	java:comp/env/jms/connectionFactory: javax.jms.ConnectionFactory for the broker
	java:comp/env/jmx/url: URL of the brokers JMX (Type java.lang.String)       
	
	
	
Master/Slave monitoring
-----------------------
To configure the web console to monitor a master/slave configuration configure the jms/jmx
as follows (system properties shown, but this option is also avaiable when using JNDI):
   -Dwebconsole.jms.url=failover:(tcp://serverA:61616,tcp://serverB:61616)
   -Dwebconsole.jmx.url=service:jmx:rmi:///jndi/rmi://serverA:1099/jmxrmi,service:jmx:rmi:///jndi/rmi://serverB:1099/jmxrmi
With this configuration the web console with switch to the slave as the master is no longer
available and back as soon as the master is back up.

