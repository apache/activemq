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
#ifndef ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ABSTRACTCOMMAND_H_
#define ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ABSTRACTCOMMAND_H_

#include <activemq/connector/stomp/StompFrame.h>
#include <activemq/connector/stomp/commands/StompCommand.h>
#include <activemq/exceptions/NullPointerException.h>
#include <activemq/util/Integer.h>
#include <activemq/util/Long.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
    
    /**
     * Interface for all Stomp commands.  Commands wrap
     * around a stomp frame and provide their own marshalling
     * to and from frames.  Stomp frame objects are dumb and have
     * a generic interface that becomes cumbersome to use directly.
     * Commands help to abstract the stomp frame by providing a
     * more user-friendly interface to the frame content.  
     */
    
    template<typename T>
    class AbstractCommand
    : 
        public StompCommand,
        public T
    {
    protected:
    
        // Frame that contains the actual message
        StompFrame* frame;

    protected:
    
        StompFrame& getFrame(void) {
            if( frame == NULL ){
                throw exceptions::NullPointerException(
                    __FILE__, __LINE__,
                    "AbstractCommand::getFrame - Frame not initialized");
            }

            return *frame;
        }
        
        const StompFrame& getFrame(void) const {
            if( frame == NULL ){
                throw exceptions::NullPointerException(
                    __FILE__, __LINE__,
                    "AbstractCommand::getFrame - Frame not initialized");
            }

            return *frame;
        }
        
        void destroyFrame(void)
        {
            if( frame != NULL ){
                delete frame;
                frame = NULL;
            }
        }
    
        const char* getPropertyValue( const std::string& name ) const{
            return getFrame().getProperties().getProperty( name );
        }

        const std::string getPropertyValue( 
            const std::string& name, 
            const std::string& defReturn ) const {
            return getFrame().getProperties().getProperty( 
                name, defReturn );
        }
        
        void setPropertyValue( const std::string& name, const std::string& value ){
            getFrame().getProperties().setProperty( name, value );
        }
        
        /**
         * Inheritors are required to override this method to init the
         * frame with data appropriate for the command type.
         * @param Frame to init
         */
        virtual void initialize( StompFrame& frame ) = 0;

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const = 0;
        
    public:
    
        AbstractCommand(void){ 
            frame = new StompFrame;
        }
        AbstractCommand(StompFrame* frame){ 
            this->frame = frame;
        }
        virtual ~AbstractCommand(void){
            destroyFrame();
        }
        
        /**
         * Sets the Command Id of this Message
         * @param Command Id
         */
        virtual void setCommandId( const unsigned int id ){
            setPropertyValue(
                CommandConstants::toString( 
                    CommandConstants::HEADER_REQUESTID),
                 util::Integer::toString( id ) );
        }

        /**
         * Gets the Command Id of this Message
         * @return Command Id
         */
        virtual unsigned int getCommandId(void) const {
            return util::Integer::parseInt(
                getPropertyValue(
                    CommandConstants::toString( 
                        CommandConstants::HEADER_REQUESTID ),
                    "0" ) );
        }
        
        /**
         * Set if this Message requires a Response
         * @param true if response is required
         */
        virtual void setResponseRequired( const bool required ) {
        }
        
        /**
         * Is a Response required for this Command
         * @return true if a response is required.
         */
        virtual bool isResponseRequired(void) const {
            return frame->getProperties().hasProperty( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_REQUESTID) );
        }
        
        /**
         * Gets the Correlation Id that is associated with this message
         * @return the Correlation Id
         */
        virtual unsigned int getCorrelationId(void) const {
            return util::Integer::parseInt(
                getPropertyValue(
                    CommandConstants::toString( 
                        CommandConstants::HEADER_RESPONSEID ),
                     "0" ) );
        }

        /**
         * Sets the Correlation Id if this Command
         * @param Id
         */
        virtual void setCorrelationId( const unsigned int corrId ) {
            setPropertyValue(
                CommandConstants::toString( 
                    CommandConstants::HEADER_RESPONSEID),
                 util::Integer::toString( corrId ) );
        }
        
        /**
         * Get the Transaction Id of this Command
         * @return the Id of the Transaction
         */      
        virtual const char* getTransactionId(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TRANSACTIONID) );
        }
      
        /**
         * Set the Transaction Id of this Command
         * @param the Id of the Transaction
         */
        virtual void setTransactionId( const std::string& id ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_TRANSACTIONID),
                id );
        }  

        /**
         * Retrieve the Stomp Command Id for this message.
         * @return Stomp CommandId enum
         */
        virtual CommandConstants::CommandId getStompCommandId(void) const {
            return CommandConstants::toCommandId(
                getFrame().getCommand() );
        }
        
        /**
         * Marshals the command to a stomp frame.
         * @returns the stomp frame representation of this
         * command.
         * @throws MarshalException if the command is not
         * in a state that can be marshaled.
         */
        virtual const StompFrame& marshal(void) const 
            throw (marshal::MarshalException)
        {
            if( frame == NULL || !validate( *frame ) ){
                throw marshal::MarshalException( 
                    __FILE__, __LINE__,
                    "AbstractCommand::marshal() - frame invalid" );
            }
            
            return getFrame();
        }

    protected:

        /**
         * Fetch the number of bytes in the Stomp Frame Body
         * @return number of bytes
         */
        virtual unsigned long getNumBytes(void) const{
            return getFrame().getBodyLength();
        }

        /**
         * Returns a char array of bytes that are contained in the message
         * @param pointer to array of bytes.
         */
        virtual const char* getBytes(void) const{
            return getFrame().getBody();
        }
    
        /**
         * Set the bytes that are to be sent in the body of this message
         * the content length flag indicates if the Content Length header
         * should be set.
         * @param bytes to store
         * @param number of bytes to pull from the bytes buffer
         * @param true if the content length header should be set
         */
        virtual void setBytes( const char* bytes, 
                               const unsigned long numBytes,
                               const bool setContentLength = true )
        {
            char* copy = new char[numBytes];
            memcpy( copy, bytes, numBytes );
            getFrame().setBody( copy, numBytes );
            if( setContentLength )
            {
                setPropertyValue( 
                    CommandConstants::toString( 
                        CommandConstants::HEADER_CONTENTLENGTH),
                    util::Long::toString( numBytes ) );
            }
        }
    };
    
}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ABSTRACTCOMMAND_H_*/
