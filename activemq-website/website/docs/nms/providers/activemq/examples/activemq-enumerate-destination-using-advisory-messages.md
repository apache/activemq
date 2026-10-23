---
title: "ActiveMQ Enumerate Destination using Advisory Messages"
---

### Enumerating Available Destinations using Advisory Messages

This example shows you how to consume Advisory Messages from the Broker to enumerate various destination types.

```
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
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;

namespace AdvisoryExample
{
    class AdvisoryExample
    {
        private IConnection connection;
        private ISession session;

        public const String QUEUE\_ADVISORY\_DESTINATION = "ActiveMQ.Advisory.Queue";
        public const String TOPIC\_ADVISORY\_DESTINATION = "ActiveMQ.Advisory.Topic";
        public const String TEMPQUEUE\_ADVISORY\_DESTINATION = "ActiveMQ.Advisory.TempQueue";
        public const String TEMPTOPIC\_ADVISORY\_DESTINATION = "ActiveMQ.Advisory.TempTopic";

        public const String ALLDEST\_ADVISORY\_DESTINATION = QUEUE\_ADVISORY\_DESTINATION + "," +
                                                           TOPIC\_ADVISORY\_DESTINATION + "," +
                                                           TEMPQUEUE\_ADVISORY\_DESTINATION + "," +
                                                           TEMPTOPIC\_ADVISORY\_DESTINATION;

        AdvisoryExample()
        {
            IConnectionFactory factory = new ConnectionFactory();

            connection = factory.CreateConnection();
            connection.Start();
            session = connection.CreateSession();
        }

        void EnumerateQueues()
        {
            Console.WriteLine("Listing all Queues on Broker:");

            IDestination dest = session.GetTopic(QUEUE\_ADVISORY\_DESTINATION);

            using(IMessageConsumer consumer = session.CreateConsumer(dest))
            {
                IMessage advisory;

                while((advisory = consumer.Receive(TimeSpan.FromMilliseconds(2000))) != null)
                {
                    ActiveMQMessage amqMsg = advisory as ActiveMQMessage;

                    if(amqMsg.DataStructure != null)
                    {
                        DestinationInfo info = amqMsg.DataStructure as DestinationInfo;
                        if(info != null)
                        {
                            Console.WriteLine("   Queue: " + info.Destination.ToString() );
                        }
                    }
                }
            }
            Console.WriteLine("Listing Complete.");
        }

        void EnumerateTopics()
        {
            Console.WriteLine("Listing all Topics on Broker:");

            IDestination dest = session.GetTopic(TOPIC\_ADVISORY\_DESTINATION);

            using(IMessageConsumer consumer = session.CreateConsumer(dest))
            {
                IMessage advisory;

                while((advisory = consumer.Receive(TimeSpan.FromMilliseconds(2000))) != null)
                {
                    ActiveMQMessage amqMsg = advisory as ActiveMQMessage;

                    if(amqMsg.DataStructure != null)
                    {
                        DestinationInfo info = amqMsg.DataStructure as DestinationInfo;
                        if(info != null)
                        {
                            Console.WriteLine("   Topic: " + info.Destination.ToString() );
                        }
                    }
                }
            }
            Console.WriteLine("Listing Complete.");
        }

        void EnumerateDestinations()
        {
            Console.WriteLine("Listing all Destinations on Broker:");

            IDestination dest = session.GetTopic(ALLDEST\_ADVISORY\_DESTINATION);

            using(IMessageConsumer consumer = session.CreateConsumer(dest))
            {
                IMessage advisory;

                while((advisory = consumer.Receive(TimeSpan.FromMilliseconds(2000))) != null)
                {
                    ActiveMQMessage amqMsg = advisory as ActiveMQMessage;

                    if(amqMsg.DataStructure != null)
                    {
                        DestinationInfo info = amqMsg.DataStructure as DestinationInfo;
                        if(info != null)
                        {
                            string destType = info.Destination.IsTopic ? "Topic" : "Qeue";
                            destType = info.Destination.IsTemporary ? "Temporary" + destType : destType;
                            Console.WriteLine("   " + destType + ": " + info.Destination.ToString() );
                        }
                    }
                }
            }
            Console.WriteLine("Listing Complete.");
        }

        void ShutDown()
        {
            session.Close();
            connection.Close();
        }

        public static void Main (string[] args)
        {
            AdvisoryExample ex = new AdvisoryExample();

            ex.EnumerateQueues();
            ex.EnumerateTopics();
            ex.EnumerateDestinations();
            ex.ShutDown();
        }
    }
}
```
