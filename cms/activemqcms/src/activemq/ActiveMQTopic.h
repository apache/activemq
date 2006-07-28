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

#ifndef ACTIVEMQ_ACTIVEMQTOPIC_H_
#define ACTIVEMQ_ACTIVEMQTOPIC_H_

#include <cms/Topic.h>
#include <string>

namespace activemq{
	
	class ActiveMQTopic : public cms::Topic
	{
	public:
	
		ActiveMQTopic( const char* name ){			
			this->name = name;
		}
		ActiveMQTopic( const std::string& name ){			
			this->name = name;
		}
		
		virtual ~ActiveMQTopic(){}
		
		/**
		 * Gets the name of this topic.
		 * @return The topic name.
		 */
		virtual const char* getTopicName() const throw( cms::CMSException ){
			return name.c_str();
		}
		
	private:
	
		std::string name;
	};
}

#endif /*ACTIVEMQ_ACTIVEMQTOPIC_H_*/
