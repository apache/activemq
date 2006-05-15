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
#include "activemq/command/ActiveMQTextMessage.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
ActiveMQTextMessage::ActiveMQTextMessage()
{
    encoder = CharsetEncoderRegistry::getEncoder() ;
    setText(NULL) ;
}

/*
 * 
 */
ActiveMQTextMessage::ActiveMQTextMessage(const char* text)
{
    encoder = CharsetEncoderRegistry::getEncoder() ;
    setText(text) ;
}

/*
 * 
 */
ActiveMQTextMessage::ActiveMQTextMessage(const char* text, const char* encname)
{
    encoder = CharsetEncoderRegistry::getEncoder(encname) ;
    setText(text) ;
}

/*
 * 
 */
ActiveMQTextMessage::~ActiveMQTextMessage()
{
}

/*
 * 
 */
unsigned char ActiveMQTextMessage::getDataStructureType()
{
    return ActiveMQTextMessage::TYPE ;
}

/*
 * 
 */
p<string> ActiveMQTextMessage::getText()
{
    // Extract text from message content
    if( this->content.size() > 0 )
    {
        p<string> value ;

        // Use undecoded string, skip string length
        value = new string( this->content.c_array() + sizeof(int), this->content.size() - sizeof(int) ) ;

        // Decode string if an encoder has been set up
        if( encoder != NULL )
            value = encoder->decode( value ) ;

        return value ;
    }
    return NULL ;
}

/*
 * 
 */
void ActiveMQTextMessage::setText(const char* text)
{
    if( text != NULL )
    {
        p<DataOutputStream>      dos ;
        p<ByteArrayOutputStream> bos ;
        p<string>                value ;
        int                      length ;

        // Set up in-memory streams
        bos = new ByteArrayOutputStream() ;
        dos = new DataOutputStream( bos ) ;

        // Encode string if an encoder has been set up
        if( encoder != NULL )
        {
            // Encode string
            value = encoder->encode( p<string> (new string(text)), &length) ;
        }
        else   // ...use unencoded string
        {
            length = (int)strlen(text) ;
            value  = new string(text) ;
        }
        // Prepend data with the string length (4 bytes)
        dos->writeInt( length ) ;
        dos->write( value->c_str(), 0, length ) ;

        // Finally, store text in content holder
        this->content = bos->toArray() ;
    }
    else
        this->content = NULL ;
}
