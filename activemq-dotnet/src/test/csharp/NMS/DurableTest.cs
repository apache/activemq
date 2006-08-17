/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Text;

using NUnit.Framework;
using ActiveMQ;
using NMS;
using ActiveMQ.Commands;
using System.Threading;

namespace ActiveMQDurableTest {
        [ TestFixture ]
        public class DurableTest
        {
                private static string TOPIC = "TestTopic";

                private static String URI = "tcp://localhost:61616";

                private static String CLIENT_ID = "DurableClientId";

                private static String CONSUMER_ID = "ConsumerId";

                private static ConnectionFactory FACTORY = new ConnectionFactory(new Uri(URI));

                private int count = 0;

                public void RegisterDurableConsumer()
                {
                        using (IConnection connection = FACTORY.CreateConnection())
                        {
                                connection.ClientId = CLIENT_ID;
                                connection.Start();

                                using (ISession session = connection.CreateSession(
                                        AcknowledgementMode.DupsOkAcknowledge))
                                {
                                        ITopic topic = session.GetTopic(TOPIC);
                                        IMessageConsumer consumer = session.CreateDurableConsumer(
                                                topic, CONSUMER_ID, "2 > 1", false);
                                        consumer.Dispose();
                                }

                                connection.Stop();
                        }
                }

                public void SendPersistentMessage()
                {
                        using (IConnection connection = FACTORY.CreateConnection())
                        {
                                connection.Start();
                                using (ISession session = connection.CreateSession(
                                        AcknowledgementMode.DupsOkAcknowledge))
                                {
                                        ITopic topic = session.GetTopic(TOPIC);
                                        ActiveMQTextMessage message = new ActiveMQTextMessage("Hello");
                                        message.NMSPersistent = true;
                                        message.Persistent = true;
                                        IMessageProducer producer = session.CreateProducer();
                                        producer.Send(topic, message);
                                        producer.Dispose();
                                }

                                connection.Stop();
                        }
                }

                [ Test ]
                public void TestMe()
                {
                        count = 0;

                        RegisterDurableConsumer();
                        SendPersistentMessage();

                        using (IConnection connection = FACTORY.CreateConnection())
                        {
                                connection.ClientId = CLIENT_ID;
                                connection.Start();

                                using (ISession session = connection.CreateSession(
                                        AcknowledgementMode.DupsOkAcknowledge))
                                {
                                        ITopic topic = session.GetTopic(TOPIC);
                                        IMessageConsumer consumer = session.CreateDurableConsumer(
                                                topic, CONSUMER_ID, "2 > 1", false);
                                        consumer.Listener += new MessageListener(consumer_Listener); /// Don't know how else to give the system enough time. /// Thread.Sleep(5000); Assert.AreEqual(0, count); Console.WriteLine("Count = " + count); SendPersistentMessage(); Thread.Sleep(5000); Assert.AreEqual(2, count); Console.WriteLine("Count = " + count); consumer.Dispose(); }

                                        connection.Stop();
                                }
                        }
                }

                /// <summary>
                ///
                /// </summary>
                /// <param name="message"></param>
                private void consumer_Listener(IMessage message)
                {
                        ++count;
                }
        }
}
