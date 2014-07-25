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
	class Listemer
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Starting up Listener.");			
						
	        String user = env("ACTIVEMQ_USER", "admin");
	        String password = env("ACTIVEMQ_PASSWORD", "password");
	        String host = env("ACTIVEMQ_HOST", "localhost");
	        int port = Int32.Parse(env("ACTIVEMQ_PORT", "61616"));
	        String destination = arg(args, 0, "event");
	
			String brokerUri = "activemq:tcp://" + host + ":" + port + "?transport.useLogging=true";
	        NMSConnectionFactory factory = new NMSConnectionFactory(brokerUri);
	
	        IConnection connection = factory.CreateConnection(user, password);
	        connection.Start();
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination dest = session.GetTopic(destination);
	
	        IMessageConsumer consumer = session.CreateConsumer(dest);
	        DateTime start = DateTime.Now;
	        long count = 0;
	        
			Console.WriteLine("Waiting for messages...");
			while (true) 
			{
	            IMessage msg = consumer.Receive();
	            if (msg is ITextMessage) 
				{
					ITextMessage txtMsg = msg as ITextMessage;
	                String body = txtMsg.Text;
	                if ("SHUTDOWN".Equals(body))
					{
	                    TimeSpan diff = DateTime.Now - start;
	                    Console.WriteLine(String.Format("Received {0} in {1} seconds", count, (1.0*diff.TotalMilliseconds/1000.0)));
	                    break;
	                } 
					else 
					{
	                    if (count == 0) 
						{
	                        start = DateTime.Now;
	                    }
	                    count ++;
	                    if (count % 1000 == 0) 
						{
	                        Console.WriteLine(String.Format("Received {0} messages.", count));
	                    }
	                }
	
	            }
				else 
				{
	                Console.WriteLine("Unexpected message type: " + msg.GetType().Name);
	            }
	        }
	        
			Console.WriteLine("Shutting down Listener.");			
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
