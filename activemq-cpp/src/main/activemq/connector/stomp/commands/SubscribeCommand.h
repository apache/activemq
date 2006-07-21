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
                    CommandConstants::HEADER_DESTINATION ) );
        }
      
        /**
         * Set the destination
         * @param the destination Name String
         */
        virtual void setDestination( const std::string& dest ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_DESTINATION ),
                dest );
        }

        /** 
         * Set the Ack Mode of this Subscription
         * @param mode setting.
         */
        virtual void setAckMode( const CommandConstants::AckMode mode ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_ACK ),
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
                        CommandConstants::HEADER_ACK ) ) );
        }
        
        /**
         * Sets the Message Selector that is associated with this
         * subscribe request
         * @param selector string
         */
        virtual void setMessageSelector( const std::string& selector ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SELECTOR ),
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
                    CommandConstants::HEADER_SELECTOR ) );
        }

        /**
         * Sets the Subscription Name that is associated with this
         * subscribe request
         * @param Subscription Name
         */
        virtual void setSubscriptionName( const std::string& name ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_SUBSCRIPTIONNAME ),
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
                    CommandConstants::HEADER_SUBSCRIPTIONNAME ) );
        }

        /**
         * Gets whether or not locally sent messages should be ignored for 
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
         * Sets whether or not locally sent messages should be ignored for 
         * subscriptions. Set to true to filter out locally sent messages
         * @param NoLocal value
         */
        virtual void setNoLocal( bool noLocal ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_NOLOCAL ),
                util::Boolean::toString( noLocal ) );
        }

        /**
         * Sets whether or not the broker is to dispatch messages in an 
         * asynchronous manner. Set to true if you want Async.
         * @return true if in dispatch async mode
         */
        virtual bool getDispatchAsync(void) const {
            return util::Boolean::parseBoolean( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_DISPATCH_ASYNC ), 
                "false" ) );
        }
      
        /**
         * Sets whether or not the broker is to dispatch messages in an 
         * asynchronous manner. Set to true if you want Async.
         * @param true for async dispatch mode
         */
        virtual void setDispatchAsync( bool dispatchAsync ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_DISPATCH_ASYNC ),
                util::Boolean::toString( dispatchAsync ) );
        }
        
        /**
         * Gets whether or not this consumer is an exclusive consumer for
         * this destination.
         * @return true for exclusive mode
         */
        virtual bool getExclusive(void) const {
            return util::Boolean::parseBoolean( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_EXCLUSIVE ), 
                "false" ) );
        }
      
        /**
         * Sets whether or not this consumer is an exclusive consumer for
         * this destination.
         * @param true if in exclusive mode
         */
        virtual void setExclusive( bool exclusive ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_EXCLUSIVE ),
                util::Boolean::toString( exclusive ) );
        }

        /**
         * Get the max number of pending messages on a destination
         * For Slow Consumer Handlingon non-durable topics by dropping old
         * messages - we can set a maximum pending limit which once a slow 
         * consumer backs up to this high water mark we begin to discard 
         * old messages
         * @return Max value
         */
        virtual int getMaxPendingMsgLimit(void) const {
            return util::Integer::parseInt( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_MAXPENDINGMSGLIMIT ), 
                "0" ) );
        }
      
        /**
         * Set the max number of pending messages on a destination
         * For Slow Consumer Handlingon non-durable topics by dropping old
         * messages - we can set a maximum pending limit which once a slow 
         * consumer backs up to this high water mark we begin to discard 
         * old messages
         * @param Max value
         */
        virtual void setMaxPendingMsgLimit( int limit ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_MAXPENDINGMSGLIMIT ),
                util::Integer::toString( limit ) );
        }
        
        /**
         * Get the maximum number of pending messages that will be 
         * dispatched to the client. Once this maximum is reached no more 
         * messages are dispatched until the client acknowledges a message. 
         * Set to 1 for very fair distribution of messages across consumers
         * where processing messages can be slow
         * @return prefetch size value
         */
        virtual int getPrefetchSize(void) const {
            return util::Integer::parseInt( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_PREFETCHSIZE ), 
                "1000" ) );
        }
      
        /**
         * Set the maximum number of pending messages that will be 
         * dispatched to the client. Once this maximum is reached no more 
         * messages are dispatched until the client acknowledges a message. 
         * Set to 1 for very fair distribution of messages across consumers
         * where processing messages can be slow
         * @param prefetch size value
         */
        virtual void setPrefetchSize( int size ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_PREFETCHSIZE ),
                util::Integer::toString( size ) );
        }

        /**
         * Gets the priority of the consumer so that dispatching can be 
         * weighted in priority order
         * @return priority level
         */
        virtual int getPriority(void) const {
            return util::Integer::parseInt( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CONSUMERPRIORITY ), 
                "0" ) );
        }
      
        /**
         * Sets the priority of the consumer so that dispatching can be 
         * weighted in priority order
         * @param prioirty level
         */
        virtual void setPriority( int priority ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CONSUMERPRIORITY ),
                util::Integer::toString( priority ) );
        }

        /**
         * Get For non-durable topics if this subscription is set to be 
         * retroactive
         * @return true for retroactive mode
         */
        virtual bool getRetroactive(void) const {
            return util::Boolean::parseBoolean( getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_RETROACTIVE ), 
                "false" ) );
        }
      
        /**
         * Set For non-durable topics if this subscription is set to be 
         * retroactive
         * @param true if in retroactive mode
         */
        virtual void setRetroactive( bool retroactive ) {
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_RETROACTIVE ),
                util::Boolean::toString( retroactive ) );
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
                    CommandConstants::HEADER_ACK ),
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
