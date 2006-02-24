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
    [ TestFixture ]
    public class ClientTest : TestSupport
    {
        [ Test ]
        public void SendAndSyncReceive()
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
                    
                    IDestination destination = session.GetQueue("FOO.BAR");
                    Assert.IsTrue(destination != null, "No queue available!");
                    
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    
                    string expected = "Hello World!";
                    ITextMessage request = session.CreateTextMessage(expected);
                    
                    producer.Send(request);
                    
                    ITextMessage message = (ITextMessage) consumer.Receive();
                    
                    Assert.IsNotNull(message, "No message returned!");
                    
                    Assert.AreEqual(expected, message.Text, "the message text");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Caught: " + e);
                }
            }
        }
    }
}

