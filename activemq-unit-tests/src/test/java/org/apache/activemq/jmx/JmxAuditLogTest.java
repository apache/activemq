/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.jmx;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import static org.apache.activemq.broker.util.JMXAuditLogEntry.VERBS;
import static org.apache.activemq.util.TestUtils.findOpenPort;

public class JmxAuditLogTest extends TestSupport
{
   protected BrokerService broker;

   protected ActiveMQQueue queue;

   int portToUse;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      setMaxTestTime(TimeUnit.MINUTES.toMillis(10));
      setAutoFail(true);

      System.setProperty("org.apache.activemq.audit", "all");

      broker = new BrokerService();
      broker.setUseJmx(true);
      broker.setDeleteAllMessagesOnStartup(true);
      portToUse = findOpenPort();
      broker.setManagementContext(createManagementContext("broker", portToUse));
      broker.setPopulateUserNameInMBeans(true);
      broker.setDestinations(createDestinations());
      TransportConnector transportConnector = broker.addConnector("tcp://0.0.0.0:0");
      transportConnector.setName("TCP");
      broker.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      System.clearProperty("org.apache.activemq.audit");
      broker.stop();
      super.tearDown();
   }

   protected ActiveMQDestination[] createDestinations()
   {
      queue = new ActiveMQQueue("myTestQueue");
      return new ActiveMQDestination[] {queue};
   }

   private MBeanServerConnection createJMXConnector(int port) throws Exception
   {
      String url = "service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi";

      Map env = new HashMap<String, String>();
      String[] creds = {"admin", "activemq"};
      env.put(JMXConnector.CREDENTIALS, creds);

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url), env);
      connector.connect();
      return connector.getMBeanServerConnection();
   }

   private ManagementContext createManagementContext(String name, int port)
   {
      ManagementContext managementContext = new ManagementContext();
      managementContext.setBrokerName(name);
      managementContext.setConnectorPort(port);
      managementContext.setConnectorHost("localhost");
      managementContext.setCreateConnector(true);

      Map<String, String> env = new HashMap<String, String>();
      env.put("jmx.remote.x.password.file", basedir + "/src/test/resources/jmx.password");
      env.put("jmx.remote.x.access.file", basedir + "/src/test/resources/jmx.access");
      managementContext.setEnvironment(env);
      return managementContext;
   }

   @Test
   public void testPasswordsAreNotLoggedWhenAuditIsTurnedOn() throws Exception
   {
      Logger log4jLogger = Logger.getLogger("org.apache.activemq.audit");
      log4jLogger.setLevel(Level.INFO);
      final AtomicInteger logCount = new AtomicInteger(0);
      final AtomicBoolean gotEnded = new AtomicBoolean(false);

      Appender appender = new DefaultTestAppender()
      {
         @Override
         public void doAppend(LoggingEvent event)
         {
            if (event.getMessage() instanceof String)
            {
               String message = (String) event.getMessage();
               System.out.println(message);
               if (message.contains("testPassword"))
               {
                  fail("Password should not appear in log file");
               }
               if (message.contains(VERBS[1])) {
                  gotEnded.set(true);
               }
            }
            logCount.incrementAndGet();
         }
      };
      log4jLogger.addAppender(appender);

      MBeanServerConnection conn = createJMXConnector(portToUse);
      ObjectName queueObjName = new ObjectName(broker.getBrokerObjectName() + ",destinationType=Queue,destinationName=" + queue.getQueueName());

      Object[] params = {"body", "testUser", "testPassword"};
      String[] signature = {"java.lang.String", "java.lang.String", "java.lang.String"};

      conn.invoke(queueObjName, "sendTextMessage", params, signature);
      log4jLogger.removeAppender(appender);

      assertTrue("got ended statement", gotEnded.get());
      assertEquals("got two messages", 2, logCount.get());

   }

   @Test
   public void testNameTargetVisible() throws Exception
   {
      Logger log4jLogger = Logger.getLogger("org.apache.activemq.audit");
      log4jLogger.setLevel(Level.INFO);
      final AtomicInteger logCount = new AtomicInteger(0);
      final AtomicBoolean gotEnded = new AtomicBoolean(false);
      final AtomicBoolean gotQueueName = new AtomicBoolean(false);
      final AtomicBoolean gotBrokerName = new AtomicBoolean(false);
      final AtomicBoolean gotConnectorName = new AtomicBoolean(false);

      final String queueName = queue.getQueueName();
      Appender appender = new DefaultTestAppender()
      {
         @Override
         public void doAppend(LoggingEvent event)
         {
            if (event.getMessage() instanceof String)
            {
               String message = (String) event.getMessage();
               System.out.println(message);
               if (message.contains(VERBS[0])) {
                  if (message.contains(queueName)) {
                     gotQueueName.set(true);
                  }
                  if (message.contains(broker.getBrokerName())) {
                     gotBrokerName.set(true);
                  }

                  if (message.contains("TCP")) {
                     gotConnectorName.set(true);
                  }
               }

               if (message.contains(VERBS[1])) {
                  gotEnded.set(true);
               }
            }
            logCount.incrementAndGet();
         }
      };
      log4jLogger.addAppender(appender);

      MBeanServerConnection conn = createJMXConnector(portToUse);
      ObjectName queueObjName = new ObjectName(broker.getBrokerObjectName() + ",destinationType=Queue,destinationName=" + queueName);

      Object[] params = {};
      String[] signature = {};

      conn.invoke(queueObjName, "purge", params, signature);

      assertTrue("got ended statement", gotEnded.get());
      assertEquals("got two messages", 2, logCount.get());
      assertTrue("got queueName in called statement", gotQueueName.get());

      // call broker to verify brokerName
      conn.invoke(broker.getBrokerObjectName(), "resetStatistics", params, signature);
      assertEquals("got 4 messages", 4, logCount.get());
      assertTrue("got brokerName in called statement", gotBrokerName.get());


      ObjectName transportConnectorON = BrokerMBeanSupport.createConnectorName(broker.getBrokerObjectName(), "clientConnectors", "TCP");
      conn.invoke(transportConnectorON, "stop", params, signature);
      assertEquals("got messages", 6, logCount.get());
      assertTrue("got connectorName in called statement", gotConnectorName.get());

      log4jLogger.removeAppender(appender);

   }
}
