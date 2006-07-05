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

#ifndef ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_SUBSCRIBECOMMAND_H_
#define ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_SUBSCRIBECOMMAND_H_

#include <activemq/connector/stomp/commands/AbstractCommand.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <activemq/transport/Command.h>
#include <activemq/util/Boolean.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
    
    /**
     * Command sent to the broker to subscribe to a topic
     * or queue.
     */
    class SubscribeCommand : public AbstractCommand< transport::Command >
    {      
    public:
      
        SubscribeCommand(void) :
            AbstractCommand<transport::Command>() {
                initialize( getFrame() );
        }
        SubscribeCommand( StompFrame* frame ) : 
            AbstractCommand< transport::Command >( frame ) {
                validate( getFrame() );
        }
        virtual ~SubscribeCommand(void) {}

        /**
         * Get the destination
         * @returns the destination Name String
         */      
        virtual const char* getDestination(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_DESTINATION) );
        }
      
        /**
         * Set the destination
         * @param the destination Name String
         */
        virtual void setDestination( const std::string& dest ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_DESTINATION),
                dest );
        }

        /** 
         * Set the Ack Mode of this Subscription
         * @param mode setting.
         */
        virtual void setAckMode( const CommandConstants::AckMode mode ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_ACK),
                CommandConstants::toString( mode ) );
        }

        /** 
         * Get the Ack Mode of this Subscription
         * @return mode setting.
         */
        virtual CommandConstants::AckMode getAckMode(void) const{
            return CommandConstants::toAckMode( 
                getPropertyValue( 
                    CommandConstants::toString( 
                        CommandConstants::HEADER_ACK) ) );
        }
        
        /**
         * Sets the Message Selector that is associated with this
         * subscribe request
         * @param selector string
         */
        virtual void setMessageSelector( const std::string& selector ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SELECTOR),
                selector );
        }        

        /**
         * Gets the Message Selector that is associated with this
         * subscribe request
         * @returns the selector string
         */      
        virtual const char* getMessageSelector(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SELECTOR) );
        }

        /**
         * Sets the Subscription Name that is associated with this
         * subscribe request
         * @param Subscription Name
         */
        virtual void setSubscriptionName( const std::string& name ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SUBSCRIPTIONNAME),
                name );
        }        

        /**
         * Gets the Subscription Name that is associated with this
         * subscribe request
         * @returns the Subscription Name
         */      
        virtual const char* getSubscriptionName(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SUBSCRIPTIONNAME) );
        }

        /**
         * Gets hether or not locally sent messages should be ignored for 
         * subscriptions. Set to true to filter out locally sent messages
         * @return NoLocal value
         */
        virtual bool getNoLocal(void) const {
            return util::Boolean::parseBoolean( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_NOLOCAL ), 
                "false" ) );
        }
      
        /**
         * Gets hether or not locally sent messages should be ignored for 
         * subscriptions. Set to true to filter out locally sent messages
         * @param NoLocal value
         */
        virtual void setNoLocal( bool noLocal ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_NOLOCAL ),
                util::Boolean::toString( noLocal ) );
        }

    protected:
    
        /**
         * Inheritors are required to override this method to init the
         * frame with data appropriate for the command type.
         * @param Frame to init
         */
        virtual void initialize( StompFrame& frame )
        {
            frame.setCommand( CommandConstants::toString(
                CommandConstants::SUBSCRIBE ) );

            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_ACK),
                CommandConstants::toString( 
                    CommandConstants::ACK_AUTO ) );
        }

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const
        {
            if((frame.getCommand() == 
                CommandConstants::toString( CommandConstants::SUBSCRIBE )) &&
               (frame.getProperties().hasProperty(
                    CommandConstants::toString( 
                        CommandConstants::HEADER_DESTINATION ) ) ) )
            {
                return true;
            }

            return false;
        }

    };

}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_SUBSCRIBECOMMAND_H_*/
