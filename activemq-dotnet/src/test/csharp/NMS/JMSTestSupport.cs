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
        
		private IConnectionFactory factory;
        private IConnection connection;
		private ISession session;
		private IDestination destination;
		
		protected int receiveTimeout = 1000;
		protected string clientId;
		protected DestinationType destinationType = DestinationType.Queue;
		protected AcknowledgementMode acknowledgementMode = AcknowledgementMode.ClientAcknowledge;
		
		[SetUp]
        virtual public void SetUp()
        {
        }
		
        [TearDown]
        virtual public void TearDown()
        {
			Disconnect();
        }
		
		// Properties
        public bool Connected
        {
            get { return connection!=null; }
            set { if( value ) Connect(); else Disconnect(); }
        }

        public IConnectionFactory Factory
        {
            get {
				if( factory == null ) {
					factory = CreateConnectionFactory();
					Assert.IsNotNull(factory, "no factory created");
				}
				return factory;
			}
            set { this.factory = value; }
        }
		
        public IConnection Connection
        {
            get {
				if( connection == null ) {
					Connect();
				}
				return connection;
			}
            set { this.connection = value; }
        }
		
        public ISession Session
        {
            get {
				if( session == null ) {
					session = Connection.CreateSession(acknowledgementMode);
					Assert.IsNotNull(connection != null, "no session created");
				}
				return session;
			}
            set { this.session = value; }
        }
		
		virtual protected void Connect()
        {
			Console.WriteLine("Connectting...");
			connection = CreateConnection();
			Assert.IsNotNull(connection, "no connection created");
			connection.Start();
			Console.WriteLine("Connected.");
			Assert.IsNotNull(connection, "no connection created");
        }
		        
        virtual protected void Disconnect()
        {
            if (connection != null)
            {
				Console.WriteLine("Disconnecting...");
                connection.Dispose();
                connection = null;
				session=null;
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
            using (ISession session = Connection.CreateSession())
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
            using (ISession session = Connection.CreateSession())
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
			IConnection connection =  Factory.CreateConnection();
			if( clientId!=null ) {
				connection.ClientId = clientId;
			}
			return connection;
		}
		
		protected virtual IMessageProducer CreateProducer()
		{
			IMessageProducer producer = Session.CreateProducer(destination);
			return producer;
		}
		
		protected virtual IMessageConsumer CreateConsumer()
		{
			IMessageConsumer consumer = Session.CreateConsumer(destination);
			return consumer;
		}
        
        protected virtual IDestination CreateDestination()
        {
			if( destinationType == DestinationType.Queue ) {
				return Session.GetQueue(CreateDestinationName());
			} else if( destinationType == DestinationType.Topic ) {
				return Session.GetTopic(CreateDestinationName());
			} else if( destinationType == DestinationType.TemporaryQueue ) {
				return Session.CreateTemporaryQueue();
			} else if( destinationType == DestinationType.TemporaryTopic ) {
				return Session.CreateTemporaryTopic();
			} else {
				throw new Exception("Unknown destination type: "+destinationType);
			}
        }
		
        protected virtual string CreateDestinationName()
        {
            return "Test.DotNet." + GetType().Name;
        }
        
        protected virtual IMessage CreateMessage()
        {
            return Session.CreateMessage();
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


