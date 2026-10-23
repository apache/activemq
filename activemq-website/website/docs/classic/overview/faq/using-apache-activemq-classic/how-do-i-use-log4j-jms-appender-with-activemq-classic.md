---
title: "How do I use log4j JMS appender with ActiveMQ"
slug: /how-do-i-use-log4j-jms-appender-with-activemq-classic
---

[Log4j JMS appender](http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/JMSAppender.html) can be used to send your log messages to JMS broker. To use ActiveMQ as a destination of your messages, you need to configure JMS appender properly. The code sample below shows example configuration:
```
log4j.rootLogger=INFO, stdout, jms

## Be sure that ActiveMQ messages are not logged to 'jms' appender
log4j.logger.org.apache.activemq=INFO, stdout

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d %-5p %c - %m%n

## Configure 'jms' appender. You'll also need jndi.properties file in order to make it work
log4j.appender.jms=org.apache.log4j.net.JMSAppender
log4j.appender.jms.InitialContextFactoryName=org.apache.activemq.jndi.ActiveMQInitialContextFactory
log4j.appender.jms.ProviderURL=tcp://localhost:61616
log4j.appender.jms.TopicBindingName=logTopic
log4j.appender.jms.TopicConnectionFactoryBindingName=ConnectionFactory
```
The important thing is not to send ActiveMQ logs to JMS appender, as it can cause errors since the broker will want to log before the connection is established. You will also need a JNDI configuration, so that appender can find appropriate topic to send log messages to. The example `jndi.properties` file can look like this:
```
topic.logTopic=logTopic
```
Finally, you can subscribe to the topic and listen for log messages:
```
public class Log4jJMSAppenderExample implements MessageListener {

	public Log4jJMSAppenderExample() throws Exception {
		// create a logTopic topic consumer
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		Connection conn = factory.createConnection();
		Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		conn.start();
		MessageConsumer consumer = sess.createConsumer(sess.createTopic("logTopic"));
		consumer.setMessageListener(this);
		// log a message
		Logger log = Logger.getLogger(Log4jJMSAppenderExample.class);
		log.info("Test log");
		// clean up
		Thread.sleep(1000);
		consumer.close();
		sess.close();
		conn.close();
		System.exit(1);
	}
	
	public static void main(String[] args) throws Exception {
		new Log4jJMSAppenderExample();
	}

	public void onMessage(Message message) {
		try {
			// receive log event in your consumer
			LoggingEvent event = (LoggingEvent)((ActiveMQObjectMessage)message).getObject();
			System.out.println("Received log [" + event.getLevel() + "]: "+ event.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
```
Note that appender send logging event wrapped in an object message, so you can extract information such as logger name, level and of course the message.

Starting with ActiveMQ 5.3, this example is included in the standard distribution. You can run it by executing
```
ant log4j-jms
```
inside `example/` folder. You can expect the following output as a result:
```
log4j-jms:
     [echo] Running a Log4j JMS Appender example
     [java] 2009-08-11 13:58:46,767 INFO  Log4jJMSAppenderExample - Test log
     [java] Received log [INFO]: Test log
     [java] Java Result: 1
```
