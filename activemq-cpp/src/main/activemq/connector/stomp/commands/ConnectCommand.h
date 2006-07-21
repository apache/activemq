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
 
#ifndef ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMAND_H_
#define ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMAND_H_

#include <activemq/connector/stomp/commands/AbstractCommand.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <activemq/transport/Command.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
    
    /**
     * Message sent to the broker to connect.
     */
    class ConnectCommand : public AbstractCommand< transport::Command >
    {
    public:

        ConnectCommand(void) :
            AbstractCommand<transport::Command>() {
                initialize( getFrame() );
        }
        ConnectCommand( StompFrame* frame ) : 
            AbstractCommand<transport::Command>( frame ) {
                validate( getFrame() );
        }
        virtual ~ConnectCommand(void) {};

        /**
         * Get the login
         * @return char* to login, can be ""
         */      
        virtual const char* getLogin(void) const {
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_LOGIN) );
        }
      
        /**
         * Set the login
         * @param password string value
         */
        virtual void setLogin( const std::string& login ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_LOGIN) ,
                login );
        }
      
        /**
         * Get the password
         * @return char* to password, can be ""
         */      
        virtual const char* getPassword(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_PASSWORD) );
        }
      
        /**
         * Set the password
         * @param passwrod string value
         */
        virtual void setPassword( const std::string& password ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_PASSWORD) ,
                password );
        }

        /**
         * Get the Client Id
         * @return char* to client Id, can be ""
         */      
        virtual const char* getClientId(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CLIENT_ID) );
        }
      
        /**
         * Set the Client Id
         * @param client id string value
         */
        virtual void setClientId( const std::string& clientId ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_CLIENT_ID) ,
                clientId );
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
                CommandConstants::CONNECT ) );
        }

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const
        {
            if(frame.getCommand() == 
               CommandConstants::toString( CommandConstants::CONNECT ) )
            {
                return true;
            }

            return false;
        }

    };
    
}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMAND_H_*/
