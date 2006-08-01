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
 
#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPMESSAGE_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPMESSAGE_H_

#include <activemq/core/ActiveMQMessage.h>
#include <activemq/core/ActiveMQAckHandler.h>
#include <activemq/connector/stomp/commands/AbstractCommand.h>
#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/StompTopic.h>

#include <activemq/util/Long.h>
#include <activemq/util/Integer.h>
#include <activemq/util/Boolean.h>

#include <string>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    /**
     * Base class for Stomp Commands that represent the Active MQ message
     * types.  This class is templated and expects the Template type to be
     * a cms::Message type, Message, TextMessage etc.  This class will 
     * implement all the general cms:Message methods
     * 
     * This class implement AbsractCommand<StompCommnd> and the 
     * ActiveMQMessage interface.
     */
    template<typename T>
    class StompMessage : 
        public AbstractCommand< transport::Command >,
        public T,
        public core::ActiveMQMessage
    {
    private:

        // Core API defined Acknowedge Handler.
        core::ActiveMQAckHandler* ackHandler;
        
        // Cached Destination
        cms::Destination* dest;
        
    public:

        StompMessage(void) :
            AbstractCommand< transport::Command >(),
            ackHandler( NULL ) { dest = NULL; }
        StompMessage( StompFrame* frame ) : 
            AbstractCommand< transport::Command >( frame ),
            ackHandler( NULL )
        {
            dest = CommandConstants::toDestination( 
                getPropertyValue(
                    CommandConstants::toString( 
                        CommandConstants::HEADER_DESTINATION ), "" ) );
        }

    	virtual ~StompMessage(void) { delete dest; }

        /**
         * Gets the properties map for this command.
         * @return Reference to a Properties object
         */
        virtual util::Properties& getProperties(void){
            return getFrame().getProperties();
        }   
        virtual const util::Properties& getProperties(void) const{
            return getFrame().getProperties();
        }   

        /**
         * Get the Correlation Id for this message
         * @return string representation of the correlation Id
         */
        virtual const char* getCMSCorrelationId(void) const {
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CORRELATIONID ) );
        }

        /**
         * Sets the Correlation Id used by this message
         * @param correlationId String representing the correlation id.
         */
        virtual void setCMSCorrelationId(const std::string& correlationId) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CORRELATIONID ) ,
                correlationId );
        }
        
        /**
         * Acknowledges all consumed messages of the session 
         * of this consumed message.
         * @throws CMSException
         */
        virtual void acknowledge(void) const throw( cms::CMSException ) {
            if(ackHandler != NULL) ackHandler->acknowledgeMessage( this );
        }

        /**
         * Sets the DeliveryMode for this message
         * @return DeliveryMode enumerated value.
         */
        virtual int getCMSDeliveryMode(void) const {
            if(!getFrame().getProperties().hasProperty( 
                   CommandConstants::toString( 
                       CommandConstants::HEADER_PERSISTANT ) ) ) {
                return cms::DeliveryMode::PERSISTANT;
            }
            
            return util::Integer::parseInt( getPropertyValue( 
                       CommandConstants::toString( 
                           CommandConstants::HEADER_PERSISTANT ) ) );
        }

        /**
         * Sets the DeliveryMode for this message
         * @param mode DeliveryMode enumerated value.
         */
        virtual void setCMSDeliveryMode( int mode ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_PERSISTANT ) ,
                util::Integer::toString( mode ) );
        }
      
        /**
         * Gets the Destination for this Message
         * @return Destination object can be NULL
         */
        virtual const cms::Destination* getCMSDestination(void) const{
            return dest;
        }
              
        /**
         * Sets the Destination for this message
         * @param destination Destination Object
         */
        virtual void setCMSDestination( const cms::Destination* destination ) {
            if( destination != NULL )
            {
                dest = destination->clone();
                setPropertyValue( 
                    CommandConstants::toString( 
                        CommandConstants::HEADER_DESTINATION ),
                    dest->toProviderString() );
            }
        }
        
        /**
         * Gets the Expiration Time for this Message
         * @return time value
         */
        virtual long getCMSExpiration(void) const {
            return util::Long::parseLong( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_EXPIRES ), "0" ) );
        }
      
        /**
         * Sets the Expiration Time for this message
         * @param expireTime time value
         */
        virtual void setCMSExpiration( long expireTime ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_EXPIRES) ,
                util::Long::toString( expireTime ) );
        }

        /**
         * Gets the CMS Message Id for this Message
         * @return time value
         */
        virtual const char* getCMSMessageId(void) const {
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_MESSAGEID ) );
        }
      
        /**
         * Sets the CMS Message Id for this message
         * @param id time value
         */
        virtual void setCMSMessageId( const std::string& id ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_MESSAGEID ),
                id );
        }
      
        /**
         * Gets the Priority Value for this Message
         * @return priority value
         */
        virtual int getCMSPriority(void) const {
            return util::Integer::parseInt( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_JMSPRIORITY ), "0" ) );
        }
      
        /**
         * Sets the Priority Value for this message
         * @param priority priority value
         */
        virtual void setCMSPriority( int priority ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_JMSPRIORITY),
                util::Integer::toString( priority ) );
        }

        /**
         * Gets the Redelivered Flag for this Message
         * @return redelivered value
         */
        virtual bool getCMSRedelivered(void) const {
            return util::Boolean::parseBoolean( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REDELIVERED ), 
                "false" ) );
        }
      
        /**
         * Sets the Redelivered Flag for this message
         * @param redelivered redelivered value
         */
        virtual void setCMSRedelivered( bool redelivered ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REDELIVERED ),
                util::Boolean::toString( redelivered ) );
        }

        /**
         * Gets the CMS Reply To Address for this Message
         * @return Reply To Value
         */
        virtual const char* getCMSReplyTo(void) const {
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REPLYTO ) );
        }

        /**
         * Sets the CMS Reply To Address for this message
         * @param id Reply To value
         */
        virtual void setCMSReplyTo( const std::string& id ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REPLYTO ),
                id );
        }

        /**
         * Gets the Time Stamp for this Message
         * @return time stamp value
         */
        virtual long getCMSTimeStamp(void) const {
            return util::Long::parseLong( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TIMESTAMP ), "0" ) );
        }
      
        /**
         * Sets the Time Stamp for this message
         * @param timeStamp time stamp value
         */
        virtual void setCMSTimeStamp( long timeStamp ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TIMESTAMP ),
                util::Long::toString( timeStamp ) );
        }

        /**
         * Gets the CMS Message Type for this Message
         * @return type value
         */
        virtual const char* getCMSMessageType(void) const {
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TYPE ) );
        }
      
        /**
         * Sets the CMS Message Type for this message
         * @param type type value
         */
        virtual void setCMSMessageType( const std::string& type ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TYPE ),
                type );
        }

    public:    // ActiveMQMessage

        /**
         * Sets the Acknowledgement Handler that this Message will use
         * when the Acknowledge method is called.
         * @param handler ActiveMQAckHandler
         */
        virtual void setAckHandler( core::ActiveMQAckHandler* handler ) {
            this->ackHandler = handler;
        }
        
        /**
         * Gets the number of times this message has been redelivered.
         * @return redelivery count
         */
        virtual int getRedeliveryCount(void) const {
            return util::Integer::parseInt( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REDELIVERYCOUNT ),
                "0" ) );
        }
        
        /**
         * Sets the count of the number of times this message has been 
         * redelivered
         * @param count redelivery count
         */
        virtual void setRedeliveryCount( int count ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REDELIVERYCOUNT ),
                util::Integer::toString( count ) );
        }

    protected:
    
        /**
         * Inheritors are required to override this method to init the
         * frame with data appropriate for the command type.
         * @param frame Frame to init
         */
        virtual void initialize( StompFrame& frame )
        {
            frame.setCommand( CommandConstants::toString(
                CommandConstants::SEND ) );
        }

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param frame Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const
        {
            if(frame.getCommand() == 
               CommandConstants::toString( CommandConstants::SEND ) )
            {
                if(frame.getProperties().hasProperty(
                    CommandConstants::toString(
                        CommandConstants::HEADER_DESTINATION ) ) )
                {
                    return true;
                }
            }
            else if( frame.getCommand() == 
                     CommandConstants::toString( CommandConstants::MESSAGE ) )
            {
                if(frame.getProperties().hasProperty(
                    CommandConstants::toString(
                        CommandConstants::HEADER_DESTINATION ) ) &&
                   frame.getProperties().hasProperty(
                    CommandConstants::toString(
                        CommandConstants::HEADER_MESSAGEID ) ) )
                {
                    return true;
                }
            }

            return false;
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPMESSAGE_H_*/
