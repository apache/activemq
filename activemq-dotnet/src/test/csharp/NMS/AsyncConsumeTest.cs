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
using NMS;
using NUnit.Framework;
using System;
using System.Threading;



namespace NMS {
        [ TestFixture ]
        public class AsyncConsumeTest : JMSTestSupport
        {
                protected Object semaphore = new Object();
                protected bool received;

                [ SetUp ]
                override public void SetUp()
                {
                        base.SetUp();
                }

                [ TearDown ]
                override public void TearDown()
                {
                        base.TearDown();
                }

                [ Test ]
                public void TestAsynchronousConsume()
                {

                        // START SNIPPET: demo
                        IMessageConsumer consumer = Session.CreateConsumer(this.Destination);
                        consumer.Listener += new MessageListener(OnMessage);
                        // END SNIPPET: demo

                        // now lets send a message
                        IMessageProducer producer = CreateProducer();
                        IMessage request = CreateMessage();
                        request.NMSCorrelationID = "abc";
                        request.NMSType = "Test";
                        producer.Send(request);

                        WaitForMessageToArrive();
                }

                [ Test ]
                public void TestCreateConsumerAfterSend()
                {
                        // now lets send a message
                        IMessageProducer producer = CreateProducer();
                        IMessage request = CreateMessage();
                        request.NMSCorrelationID = "abc";
                        request.NMSType = "Test";
                        producer.Send(request);

                        // lets create an async consumer
                        IMessageConsumer consumer = Session.CreateConsumer(this.Destination);
                        consumer.Listener += new MessageListener(OnMessage);
                        
                        WaitForMessageToArrive();
                }

                [ Test ]
                public void TestCreateConsumerBeforeSendButAddListenerAfterSend()
                {
                        // lets create an async consumer
                        IMessageConsumer consumer = Session.CreateConsumer(this.Destination);
                        
                        // now lets send a message
                        IMessageProducer producer = CreateProducer();
                        IMessage request = CreateMessage();
                        request.NMSCorrelationID = "abc";
                        request.NMSType = "Test";
                        producer.Send(request);

                        // now lets add the listener
                        consumer.Listener += new MessageListener(OnMessage);
                        
                        WaitForMessageToArrive();
                }

                [ Test ]
                public void TextMessageSRExample()
                {
                        using (IConnection connection = Factory.CreateConnection())
                        {
                                AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
                                ISession session = connection.CreateSession(acknowledgementMode);

                                IDestination destination = session.GetQueue("FOO.BAR");

                                // lets create a consumer and producer
                                IMessageConsumer consumer = session.CreateConsumer(destination);
                                consumer.Listener += new MessageListener(OnMessage);

                                IMessageProducer producer = session.CreateProducer(destination);
                                producer.Persistent = true;

                                // lets send a message
                                ITextMessage request = session.CreateTextMessage(
                                        "HelloWorld!");
                                request.NMSCorrelationID = "abc";
                                request.Properties["JMSXGroupID"] = "cheese";
                                request.Properties["myHeader"] = "James";

                                producer.Send(request);

                                WaitForMessageToArrive();
                        }
                }

                protected void OnMessage(IMessage message)
                {
                        Console.WriteLine("Received message: " + message);
                        lock (semaphore)
                        {
                                received = true;
                                Monitor.PulseAll(semaphore);
                        }
                }

                protected void WaitForMessageToArrive()
                {
                        lock (semaphore)
                        {
                                if (!received)
                                {
                                        Monitor.Wait(semaphore, receiveTimeout);
                                }
                                Assert.AreEqual(true, received, "Should have received a message by now!");
                        }
                }
        }
}
