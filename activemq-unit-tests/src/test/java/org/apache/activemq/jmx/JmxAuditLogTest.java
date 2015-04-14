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

import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

public class JmxAuditLogTest extends TestSupport
{
   protected BrokerService broker;

   protected ActiveMQQueue queue;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      setMaxTestTime(TimeUnit.MINUTES.toMillis(10));
      setAutoFail(true);

      System.setProperty("org.apache.activemq.audit", "true");

      broker = new BrokerService();
      broker.setUseJmx(true);
      broker.setManagementContext(createManagementContext("broker", 1099));
      broker.setPopulateUserNameInMBeans(true);
      broker.setDestinations(createDestinations());
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

      Appender appender = new DefaultTestAppender()
      {
         @Override
         public void doAppend(LoggingEvent event)
         {
            if (event.getMessage() instanceof String)
            {
               String message = (String) event.getMessage();
               if (message.contains("testPassword"))
               {
                  fail("Password should not appear in log file");
               }
            }
         }
      };
      log4jLogger.addAppender(appender);

      MBeanServerConnection conn = createJMXConnector(1099);
      ObjectName queueObjName = new ObjectName(broker.getBrokerObjectName() + ",destinationType=Queue,destinationName=" + queue.getQueueName());

      Object[] params = {"body", "testUser", "testPassword"};
      String[] signature = {"java.lang.String", "java.lang.String", "java.lang.String"};

      conn.invoke(queueObjName, "sendTextMessage", params, signature);
      log4jLogger.removeAppender(appender);
   }
}
