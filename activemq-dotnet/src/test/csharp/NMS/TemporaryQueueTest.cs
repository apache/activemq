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
    public class TemporaryQueueTest : JMSTestSupport {
        protected Object semaphore = new Object();
        protected bool received;

        [ SetUp ]
        override public void SetUp() {
            base.SetUp();
        }

        [ TearDown ]
        override public void TearDown() {
            base.TearDown();
        }

        [ Test ]
        public void TestAsynchronousConsume() {
            // lets consume to a regular queue
            IMessageConsumer consumer = CreateConsumer();
            consumer.Listener += new MessageListener(OnQueueMessage);

            // lets create a temporary queue and a consumer on it
            ITemporaryQueue tempQ = Session.CreateTemporaryQueue();
            IMessageConsumer tempQueueConsumer = Session.CreateConsumer(tempQ);
            tempQueueConsumer.Listener += new MessageListener(OnTempQueueMessage);

            // Send message to queue which has a listener to reply to the temporary queue
            IMessageProducer producer = CreateProducer();
            
            IMessage request = CreateMessage();
            request.NMSCorrelationID = "abc";
            request.NMSReplyTo = tempQ;
            request.NMSPersistent = false;
            producer.Send(request);

            // now lets wait for the message to arrive on the temporary queue
            WaitForMessageToArrive();
        }

        protected void OnQueueMessage(IMessage message) {
			Console.WriteLine("First message received: " + message + " so about to reply to temporary queue");

            ITextMessage response = Session.CreateTextMessage("this is a response!!");
            response.NMSCorrelationID = message.NMSCorrelationID;

            IMessageProducer producerTempQ = Session.CreateProducer(message.NMSReplyTo);
            //Write msg to temp q.
            producerTempQ.Send(response); 
        }
        
        protected void OnTempQueueMessage(IMessage message) {
            Console.WriteLine("Received message on temporary queue: " + message);
            lock (semaphore) {
                received = true;
                Monitor.PulseAll(semaphore);
            }
        }

        protected void WaitForMessageToArrive() {
            lock (semaphore) {
                if (!received) {
                    Monitor.Wait(semaphore, receiveTimeout);
                }
                Assert.AreEqual(true, received, "Should have received a message by now!");
            }
        }
    }
}
