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

#ifndef ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMEWRAPPER_H_
#define ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMEWRAPPER_H_
 
#include <string>
#include <map>
#include <activemq/util/SimpleProperties.h>

namespace activemq{
namespace connector{
namespace stomp{

    /**
     * A Stomp-level message frame that encloses all messages
     * to and from the broker.
     */
    class StompFrame{           
    public:
    
        /**
         * Default constructor.
         */
        StompFrame(void){
            body = NULL;
            bodyLength = 0;
        }
        
        /**
         * Destruction - frees the memory pool.
         */
        virtual ~StompFrame(void) { delete body; }
        
        /**
         * Clonse this message exactly, returns a new instance that the
         * caller is required to delete.
         * @return new copy of this message
         */
        virtual StompFrame* clone(void) const {
            StompFrame* frame = new StompFrame();
            frame->command = command;
            frame->properties = properties;
            char* cpyBody = new char[bodyLength];
            memcpy(cpyBody, body, bodyLength);
            frame->setBody(cpyBody, bodyLength);
            return frame;
        }
        
        /**
         * Sets the command for this stomp frame.
         * @param command The command to be set.
         */
        void setCommand( const std::string& cmd ){
            this->command = cmd;
        }
        
        /**
         * Accessor for this frame's command field.
         */
        const std::string& getCommand(void) const{
            return command;
        }
        
        /**
         * Gets access to the header properties for this frame.
         */
        util::Properties& getProperties(void){ return properties; }
        const util::Properties& getProperties(void) const { 
            return properties;
        }
        
        /**
         * Accessor for the body data of this frame.
         * @return char pointer to body data
         */
        const char* getBody(void) const{
            return body;
        }
        
        /**
         * Return the number of bytes contained in this frames body
         * @return Body bytes length.
         */
        int getBodyLength(void) const{ return bodyLength; }
        
        /**
         * Sets the body data of this frame as a byte sequence.
         * @param bytes The byte buffer to be set in the body.
         * @param numBytes The number of bytes in the buffer.
         */
        void setBody( const char* bytes, const int numBytes ){
            body = bytes;
            bodyLength = numBytes;
        }   
        
    private:

        // String Name of this command.
        std::string command;

        // Properties of the Stomp Message
        util::SimpleProperties properties;

        // Byte data of Body.
        const char* body;
        int bodyLength;     
        
    };
    
}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMEWRAPPER_H_*/
