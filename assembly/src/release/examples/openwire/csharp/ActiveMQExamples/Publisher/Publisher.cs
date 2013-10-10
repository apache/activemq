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
using Apache.NMS.ActiveMQ;

namespace ActiveMQ.Example
{
	class Publisher
	{
		public static void Main (string[] args)
		{
	        String user = env("ACTIVEMQ_USER", "admin");
	        String password = env("ACTIVEMQ_PASSWORD", "password");
	        String host = env("ACTIVEMQ_HOST", "localhost");
	        int port = Int32.Parse(env("ACTIVEMQ_PORT", "61616"));
			String destination = arg(args, 0, "event");
	
	        int messages = 10000;
	        int size = 256;
	
	        String DATA = "abcdefghijklmnopqrstuvwxyz";
	        String body = "";
	        for(int i=0; i < size; i ++) 
			{
	            body += DATA[i%DATA.Length];
	        }
	
			String brokerUri = "activemq:tcp://" + host + ":" + port;
	        NMSConnectionFactory factory = new NMSConnectionFactory(brokerUri);
	
	        IConnection connection = factory.CreateConnection(user, password);
	        connection.Start();
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination dest = session.GetTopic(destination);
	        IMessageProducer producer = session.CreateProducer(dest);
	        producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
	
	        for (int i=1; i <= messages; i ++) 
			{
	            producer.Send(session.CreateTextMessage(body));
	            if ((i % 1000) == 0) 
				{
	                Console.WriteLine(String.Format("Sent {0} messages", i));
	            }
	        }
	
	        producer.Send(session.CreateTextMessage("SHUTDOWN"));
	        connection.Close();
		}

	    private static String env(String key, String defaultValue)
		{
	        String rc = System.Environment.GetEnvironmentVariable(key);
	        if (rc == null)
			{
	            return defaultValue;
			}
	        return rc;
	    }
	
	    private static String arg(String []args, int index, String defaultValue) 
		{
	        if (index < args.Length)
			{
	            return args[index];
			}
            return defaultValue;
	    }
	}
}
