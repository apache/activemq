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
    
    /// <summary>
    /// useful base class for test cases
    /// </summary>
    [ TestFixture ]
    public abstract class TestSupport
    {
        
        public virtual void SendAndSyncReceive()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            
            Assert.IsTrue(factory != null, "no factory created");
            
            using (IConnection connection = factory.CreateConnection())
            {
                try
                {
                    Assert.IsTrue(connection != null, "no connection created");
                    Console.WriteLine("Connected to ActiveMQ!");
                    
                    ISession session = connection.CreateSession();
                    
                    IDestination destination = CreateDestination(session);
                    Assert.IsTrue(destination != null, "No queue available!");
                    
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    
                    IMessage request = CreateMessage(session);
                    
                    producer.Send(request);
                    
                    IMessage message = consumer.Receive();
                    Assert.IsNotNull(message, "No message returned!");
                    
                    AssertValidMessage(message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Caught: " + e);
                }
            }
        }

        protected virtual IDestination CreateDestination(ISession session)
        {
            string name = "Test.DotNet." + GetType().Name;
            IDestination destination = session.GetQueue(name);
            
            Console.WriteLine("Using queue: " + destination);
            return destination;
        }
        
        protected virtual IMessage CreateMessage(ISession session) {
            return session.CreateMessage();
        }
        
        protected virtual  void AssertValidMessage(IMessage message) {
            Assert.IsNotNull(message, "Null Message!");
        }
    }
}
