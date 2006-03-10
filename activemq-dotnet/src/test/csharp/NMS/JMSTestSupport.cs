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
using NMS;
using NUnit.Framework;
using System;

/// <summary>
/// useful base class for test cases
/// </summary>

namespace NMS
{
	[ TestFixture ]
    public abstract class JMSTestSupport
    {
        
		protected IConnectionFactory factory;
        protected IConnection connection;
		protected ISession session;
		private IDestination destination;
		protected int receiveTimeout = 1000;
		protected AcknowledgementMode acknowledgementMode = AcknowledgementMode.ClientAcknowledge;
		
		[SetUp]
        virtual public void SetUp()
        {
			Connect();
        }
		
        [TearDown]
        virtual public void TearDown()
        {
			Disconnect();
        }
		
		
		virtual protected void Connect()
        {
			Console.WriteLine("Connectting...");
            factory = CreateConnectionFactory();
            Assert.IsNotNull(factory, "no factory created");
            connection = CreateConnection();
            Assert.IsNotNull(connection, "no connection created");
			connection.Start();
			session = connection.CreateSession(acknowledgementMode);
            Assert.IsNotNull(connection != null, "no session created");
			Console.WriteLine("Connected.");
        }
		
        
        virtual protected void Disconnect()
        {
            if (connection != null)
            {
				Console.WriteLine("Disconnecting...");
                connection.Dispose();
                connection = null;
				Console.WriteLine("Disconnected.");
            }
        }
        
        virtual protected void Reconnect()
        {
            Disconnect();
            Connect();
        }
		
		protected virtual void Drain()
		{
            using (ISession session = connection.CreateSession())
            {
				// Tries to consume any messages on the Destination
				IMessageConsumer consumer = session.CreateConsumer(Destination);
				
				// Should only need to wait for first message to arrive due to the way
				// prefetching works.
				IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(receiveTimeout));
				while (msg != null)
				{
					msg = consumer.ReceiveNoWait();
				}
			}
		}
		
        public virtual void SendAndSyncReceive()
        {
            using (ISession session = connection.CreateSession())
            {
				
				IMessageConsumer consumer = session.CreateConsumer(Destination);
				IMessageProducer producer = session.CreateProducer(Destination);
				
				IMessage request = CreateMessage();
				producer.Send(request);
				
				IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(receiveTimeout));
                Assert.IsNotNull(message, "No message returned!");
                AssertValidMessage(message);
            }
        }
		
		protected virtual IConnectionFactory CreateConnectionFactory()
		{
			return new ActiveMQ.ConnectionFactory(new Uri("tcp://localhost:61616"));
		}
		
		protected virtual IConnection CreateConnection()
		{
			return factory.CreateConnection();
		}
		
		protected virtual IMessageProducer CreateProducer()
		{
			IMessageProducer producer = session.CreateProducer(destination);
			return producer;
		}
		
		protected virtual IMessageConsumer CreateConsumer()
		{
			IMessageConsumer consumer = session.CreateConsumer(destination);
			return consumer;
		}
        
        protected virtual IDestination CreateDestination()
        {
            return session.GetQueue(CreateDestinationName());
        }
		
        protected virtual string CreateDestinationName()
        {
            return "Test.DotNet." + GetType().Name;
        }
        
        protected virtual IMessage CreateMessage()
        {
            return session.CreateMessage();
        }
        
        protected virtual  void AssertValidMessage(IMessage message)
        {
            Assert.IsNotNull(message, "Null Message!");
        }
		
		
        public IDestination Destination
        {
            get {
				if (destination == null)
				{
					destination = CreateDestination();
					Assert.IsNotNull(destination, "No destination available!");
					Console.WriteLine("Using destination: " + destination);
				}
				return destination;
			}
            set {
				destination = value;
            }
        }
		
    }
}


