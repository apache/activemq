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

#ifndef ACTIVEMQ_TRANSPORT_TOPICLISTENER_H_
#define ACTIVEMQ_TRANSPORT_TOPICLISTENER_H_

#include <cms/ExceptionListener.h>
#include <cms/Message.h>

namespace activemq{
namespace transport{
	
    /**
     * A listener of topic events from a transport object.
     * @author Nathan Mittler
     */
	class TopicListener{
		
	public:
		
		virtual ~TopicListener(){}
        
        /**
         * Invoked when a client topic message is received by the
         * transport layer.
         * @param topic The topic on which the message was
         * received.
         * @param message The message received.
         */
        virtual void onTopicMessage( const cms::Topic* topic,
            const cms::Message* message );
	};
    
}}

#endif /*ACTIVEMQ_TRANSPORT_TOPICLISTENER_H_*/
