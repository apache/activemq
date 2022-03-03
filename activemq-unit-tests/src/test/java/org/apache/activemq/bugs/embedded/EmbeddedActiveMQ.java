/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs.embedded;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedActiveMQ
{
 
        private static Logger logger = LoggerFactory.getLogger(EmbeddedActiveMQ.class);
 
        public static void main(String[] args)
        {
 
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                BrokerService brokerService = null;
                Connection connection = null;
 
                logger.info("Start...");
                try
                {
                        brokerService = new BrokerService();
                        brokerService.setBrokerName("TestMQ");
                        brokerService.setUseJmx(true);
                        logger.info("Broker '" + brokerService.getBrokerName() + "' is starting........");
                        brokerService.start();
                        ConnectionFactory fac = new ActiveMQConnectionFactory("vm://TestMQ");
                        connection = fac.createConnection();
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Destination queue = session.createQueue("TEST.QUEUE");
                        MessageProducer producer = session.createProducer(queue);
                        for (int i = 0; i < 1000;i++) {
                            Message msg = session.createTextMessage("test"+i);
                            producer.send(msg);
                        }
                        logger.info(ThreadExplorer.show("Active threads after start:"));
                        System.out.println("Press return to stop........");
                        String key = br.readLine();
                }
 
                catch (Exception e)
                {
                        e.printStackTrace();
                }
                finally
                {
                        try
                        {
                                br.close();
                                logger.info("Broker '" + brokerService.getBrokerName() + "' is stopping........");
                                connection.close();
                                brokerService.stop(); 
                                sleep(8);
                                logger.info(ThreadExplorer.show("Active threads after stop:"));
 
                        }
                        catch (Exception e)
                        {
                                e.printStackTrace();
                        }
                }
 
                logger.info("Waiting for list theads is greater then 1 ...");
                int numTh = ThreadExplorer.active();
 
                while (numTh > 2)
                {
                        sleep(3);
                        numTh = ThreadExplorer.active();
                        logger.info(ThreadExplorer.show("Still active threads:"));
                }
 
                System.out.println("Stop...");
        }
 
        private static void sleep(int second)
        {
                try
                {
                        logger.info("Waiting for " + second + "s...");
                        Thread.sleep(second * 1000L);
                }
                catch (InterruptedException e)
                {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
        }
 
}
