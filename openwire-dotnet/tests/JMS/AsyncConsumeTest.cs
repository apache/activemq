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
using JMS;
using NUnit.Framework;
using System;
using System.Threading;


namespace JMS
{
	[TestFixture]
    public class AsyncConsumeTest : JMSTestSupport
    {
        protected Object semaphore = new Object();
        protected bool received;
        
		[SetUp]
        override public void SetUp()
        {
			base.SetUp();
        }
		
        [TearDown]
        override public void TearDown()
        {
			base.TearDown();
        }
		
        [Test]
        public void TestAsynchronousConsume()
        {
                
			// lets create an async consumer
			// START SNIPPET: demo
			IMessageConsumer consumer = session.CreateConsumer(this.Destination);
			consumer.Listener += new MessageListener(OnMessage);
			// END SNIPPET: demo
			
			// now lets send a message
			IMessageProducer producer = CreateProducer();
			IMessage request = CreateMessage();
			request.JMSCorrelationID = "abc";
			request.JMSType = "Test";
			producer.Send(request);
			
			WaitForMessageToArrive();
            
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

