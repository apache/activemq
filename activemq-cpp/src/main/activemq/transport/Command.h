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
 
#ifndef ACTIVEMQ_TRANSPORT_COMMAND_H_
#define ACTIVEMQ_TRANSPORT_COMMAND_H_

namespace activemq{
namespace transport{
  
    class Command{
    public:
  
        virtual ~Command(void){}
        
        /**
         * Sets the Command Id of this Message
         * @param id Command Id
         */
        virtual void setCommandId( const unsigned int id ) = 0;

        /**
         * Gets the Command Id of this Message
         * @return Command Id
         */
        virtual unsigned int getCommandId() const = 0;
        
        /**
         * Set if this Message requires a Response
         * @param required true if response is required
         */
        virtual void setResponseRequired( const bool required ) = 0;

        /**
         * Is a Response required for this Command
         * @return true if a response is required.
         */
        virtual bool isResponseRequired() const = 0;
        
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_COMMAND_H_*/
