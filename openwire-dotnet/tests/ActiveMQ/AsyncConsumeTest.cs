/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
using System.IO;
using System.Threading;

using NUnit.Framework;

namespace ActiveMQ
{
    [TestFixture]
    public class AsyncConsumeTest : TestSupport
    {
        protected Object semaphore = new Object();
        protected bool received;
        
        [Test]
        public void TestAsynchronousConsume()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            Assert.IsTrue(factory != null, "no factory created");
            
            using (IConnection connection = factory.CreateConnection())
            {
                Assert.IsTrue(connection != null, "no connection created");
                Console.WriteLine("Connected to ActiveMQ!");
                
                ISession session = connection.CreateSession();
                IDestination destination = CreateDestination(session);
                Assert.IsTrue(destination != null, "No queue available!");
                
                // lets create an async consumer
                // START SNIPPET: demo
                IMessageConsumer consumer = session.CreateConsumer(destination);
                consumer.Listener += new MessageListener(OnMessage);
                // END SNIPPET: demo
                
                
                // now lets send a message
                session = connection.CreateSession();
                IMessageProducer producer = session.CreateProducer(destination);
                IMessage request = CreateMessage(session);
                request.JMSCorrelationID = "abc";
                request.JMSType = "Test";
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
                    Monitor.Wait(semaphore, 10000);
                }
            }
            Assert.AreEqual(true, received, "Should have received a message by now!");
        }
        
    }
}
