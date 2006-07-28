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
using NMS;
using System;

namespace ActiveMQ.Commands
{
	
	/// <summary>
	/// Summary description for ActiveMQTopic.
	/// </summary>
	public class ActiveMQTopic : ActiveMQDestination, ITopic
    {
        public const byte ID_ActiveMQTopic = 101;
        
        public ActiveMQTopic() : base()
        {
        }
        public ActiveMQTopic(String name) : base(name)
        {
        }
		
		override public DestinationType DestinationType
		{
			get {
				return DestinationType.Topic;
			}
		}
		
        
        public String TopicName
        {
            get { return PhysicalName; }
        }
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQTopic;
        }
        
        public override int GetDestinationType()
        {
            return ACTIVEMQ_TOPIC;
        }
        
        public override ActiveMQDestination CreateDestination(String name)
        {
            return new ActiveMQTopic(name);
        }
    }
}

