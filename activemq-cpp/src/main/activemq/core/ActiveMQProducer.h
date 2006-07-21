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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQPRODUCER_H_
#define _ACTIVEMQ_CORE_ACTIVEMQPRODUCER_H_

#include <cms/MessageProducer.h>
#include <cms/Message.h>
#include <cms/Destination.h>
#include <cms/DeliveryMode.h>

#include <activemq/core/ActiveMQSessionResource.h>
#include <activemq/connector/ProducerInfo.h>

namespace activemq{
namespace core{

    class ActiveMQSession;

    class ActiveMQProducer : public cms::MessageProducer,
                             public ActiveMQSessionResource
    {
    private:
   
        // Delivery Mode of this Producer
        int deliveryMode;
      
        // Disable the Message Id
        bool disableMsgId;
      
        // Disable sending timestamps
        bool disableTimestamps;
      
        // Priority Level to send at
        int priority;

        // Time to live setting for message
        int timeToLive;
      
        // Session that this producer sends to.
        ActiveMQSession* session;
      
        // This Producers protocal specific info object
        connector::ProducerInfo* producerInfo;
      
    public:

        /**
         * Constructor
         */
        ActiveMQProducer( connector::ProducerInfo* producerInfo,
                          ActiveMQSession* session );

        virtual ~ActiveMQProducer(void);

        /**
         * Sends the message to the default producer destination.
         * @param a Message Object Pointer
         * @throws CMSException
         */
        virtual void send( cms::Message* message ) throw ( cms::CMSException );
      
        /**
         * Sends the message to the designated destination.
         * @param a Message Object Pointer
         * @throws CMSException
         */
        virtual void send( const cms::Destination* destination,
                           cms::Message* message ) throw ( cms::CMSException );

        /** 
         * Sets the delivery mode for this Producer
         * @param The DeliveryMode
         */
        virtual void setDeliveryMode( int mode ) {
            deliveryMode = mode; 
        }
      
        /** 
         * Gets the delivery mode for this Producer
         * @return The DeliveryMode
         */
        virtual int getDeliveryMode(void) const {
            return deliveryMode; 
        }
      
        /**
         * Sets if Message Ids are disbled for this Producer
         * @param boolean indicating enable / disable (true / false)
         */
        virtual void setDisableMessageId( bool value ) {
            disableMsgId = value; 
        }
      
        /**
         * Sets if Message Ids are disbled for this Producer
         * @param boolean indicating enable / disable (true / false)
         */
        virtual bool getDisableMessageId(void) const {
            return disableMsgId;
        }

        /**
         * Sets if Message Time Stamps are disbled for this Producer
         * @param boolean indicating enable / disable (true / false)
         */
        virtual void setDisableMessageTimeStamp( bool value ) {
            disableTimestamps = value;
        }
      
        /**
         * Sets if Message Time Stamps are disbled for this Producer
         * @param boolean indicating enable / disable (true / false)
         */
        virtual bool getDisableMessageTimeStamp(void) const {
            return disableTimestamps;
        }
      
        /**
         * Sets the Priority that this Producers sends messages at
         * @param int value for Priority level
         */
        virtual void setPriority( int priority ) {
            this->priority = priority; 
        }
      
        /**
         * Gets the Priority level that this producer sends messages at
         * @return int based priority level
         */
        virtual int getPriority(void) const {
            return priority;
        }
      
        /**
         * Sets the Time to Live that this Producers sends messages with
         * @param int value for time to live
         */
        virtual void setTimeToLive( int time ) {
            timeToLive = time;
        }
  
        /**
         * Gets the Time to Live that this producer sends messages with
         * @return int based Time to Live
         */
        virtual int getTimeToLive(void) const {
            return timeToLive;
        }
      
    public:  // ActiveMQSessionResource
    
        /**
         * Retrieve the Connector resource that is associated with
         * this Session resource.
         * @return pointer to a Connector Resource, can be NULL
         */
        virtual connector::ConnectorResource* getConnectorResource(void) {
            return producerInfo;
        }

    public:
   
        /**
         * Retrives this object ProducerInfo pointer
         * @return ProducerInfo pointer
         */
        virtual connector::ProducerInfo* getProducerInfo(void){
            return producerInfo;
        }

   };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQPRODUCER_H_*/
