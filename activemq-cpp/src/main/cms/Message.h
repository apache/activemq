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

#ifndef _CMS_MESSAGE_H_
#define _CMS_MESSAGE_H_

#include <activemq/util/Properties.h>

#include <cms/Destination.h>
#include <cms/CMSException.h>
#include <cms/DeliveryMode.h>

namespace cms{
   
    /**
     * Root of all messages.
     */
    class Message
    {         
    public:
   
        virtual ~Message(void){}
      
        /**
         * Clonse this message exactly, returns a new instance that the
         * caller is required to delete.
         * @return new copy of this message
         */
        virtual Message* clone(void) const = 0;
        
        /**
         * Acknowledges all consumed messages of the session 
         * of this consumed message.
         */
        virtual void acknowledge(void) const throw( CMSException ) = 0;
      
        /**
         * Retrieves a reference to the properties object owned
         * by this message
         * @return A Properties Object reference
         */
        virtual activemq::util::Properties& getProperties(void) = 0;
        virtual const activemq::util::Properties& getProperties(void) const = 0;
      
        /**
         * Get the Correlation Id for this message
         * @return string representation of the correlation Id
         */
        virtual const char* getCMSCorrelationId(void) const = 0;

        /**
         * Sets the Correlation Id used by this message
         * @param correlationId - String representing the correlation id.
         */
        virtual void setCMSCorrelationId( const std::string& correlationId ) = 0;

        /**
         * Gets the DeliveryMode for this message
         * @return DeliveryMode enumerated value.
         */
        virtual int getCMSDeliveryMode(void) const = 0;

        /**
         * Sets the DeliveryMode for this message
         * @param mode - DeliveryMode enumerated value.
         */
        virtual void setCMSDeliveryMode( int mode ) = 0;
      
        /**
         * Gets the Destination for this Message, returns a
         * @return Destination object
         */
        virtual const Destination* getCMSDestination(void) const = 0;
      
        /**
         * Sets the Destination for this message
         * @param destination - Destination Object
         */
        virtual void setCMSDestination( const Destination* destination ) = 0;
      
        /**
         * Gets the Expiration Time for this Message
         * @return time value
         */
        virtual long getCMSExpiration(void) const = 0;
      
        /**
         * Sets the Expiration Time for this message
         * @param expireTime - time value
         */
        virtual void setCMSExpiration( long expireTime ) = 0;
      
        /**
         * Gets the CMS Message Id for this Message
         * @return time value
         */
        virtual const char* getCMSMessageId(void) const = 0;
      
        /**
         * Sets the CMS Message Id for this message
         * @param id - time value
         */
        virtual void setCMSMessageId( const std::string& id ) = 0;
      
        /**
         * Gets the Priority Value for this Message
         * @return priority value
         */
        virtual int getCMSPriority(void) const = 0;
      
        /**
         * Sets the Priority Value for this message
         * @param priority - priority value for this message
         */
        virtual void setCMSPriority( int priority ) = 0;

        /**
         * Gets the Redelivered Flag for this Message
         * @return redelivered value
         */
        virtual bool getCMSRedelivered(void) const = 0;
      
        /**
         * Sets the Redelivered Flag for this message
         * @param redelivered - boolean redelivered value
         */
        virtual void setCMSRedelivered( bool redelivered ) = 0;

        /**
         * Gets the CMS Reply To Address for this Message
         * @return Reply To Value
         */
        virtual const char* getCMSReplyTo(void) const = 0;
      
        /**
         * Sets the CMS Reply To Address for this message
         * @param id - Reply To value
         */
        virtual void setCMSReplyTo( const std::string& id ) = 0;

        /**
         * Gets the Time Stamp for this Message
         * @return time stamp value
         */
        virtual long getCMSTimeStamp(void) const = 0;
      
        /**
         * Sets the Time Stamp for this message
         * @param timeStamp - integer time stamp value
         */
        virtual void setCMSTimeStamp( long timeStamp ) = 0;

        /**
         * Gets the CMS Message Type for this Message
         * @return type value
         */
        virtual const char* getCMSMessageType(void) const = 0;
      
        /**
         * Sets the CMS Message Type for this message
         * @param type - message type value string
         */
        virtual void setCMSMessageType( const std::string& type ) = 0;
    };
}

#endif /*_CMS_MESSAGE_H_*/
