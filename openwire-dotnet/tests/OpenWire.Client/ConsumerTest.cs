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

using NUnit.Framework;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    [TestFixture]
    public class ConsumerTest : TestSupport
    {
		IConnectionFactory factory;
		IConnection connection;
		IDestination destination;

		[SetUp]
		protected void SetUp() 
		{
			factory = new ConnectionFactory("localhost", 61616);
			connection = factory.CreateConnection();
		}

		[TearDown]
		protected void TearDown() 
		{
			connection.Dispose();
		}

        [Test]
		[Ignore("Not fully implemented yet.")]
		public void testDurableConsumerSelectorChange()  
		{

			// Receive a message with the JMS API
			connection.ClientId="test";
			connection.Start();
			
			ISession session = connection.CreateSession(false, AcknowledgementMode.AutoAcknowledge);
			destination = session.GetTopic("foo");
			IMessageProducer producer = session.CreateProducer(destination);
			producer.Persistent = true;
			IMessageConsumer consumer = session.CreateDurableConsumer((ITopic)destination, "test", "color='red'", false);

			// Send the messages
			ITextMessage message = session.CreateTextMessage("1st");
			//message.SetStringProperty("color", "red");
			producer.Send(message);
	        
			IMessage m = consumer.Receive(1000);
			Assert.IsNotNull(m);
			Assert.AreEqual("1st", ((ITextMessage)m).Text );

			// Change the subscription.
			consumer.Dispose();
			consumer = session.CreateDurableConsumer((ITopic)destination, "test", "color='blue'", false);
	        
			message = session.CreateTextMessage("2nd");
			// message.setStringProperty("color", "red");
			producer.Send(message);
			message = session.CreateTextMessage("3rd");
			 // message.setStringProperty("color", "blue");
			producer.Send(message);

			// Selector should skip the 2nd message.
			m = consumer.Receive(1000);
			Assert.IsNotNull(m);
			Assert.AreEqual("3rd", ((ITextMessage)m).Text);
	        
			Assert.IsNull(consumer.ReceiveNoWait());
		}

    }
}

