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
    setText(NULL) ;
}

/*
 * 
 */
ActiveMQTextMessage::ActiveMQTextMessage(const char* text)
{
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
        int utflen = 0 ;
        char* buffer = this->content.c_array() ;

        // TODO: assuming that the text is ASCII
        utflen |= (char) ((buffer[0] << 24) & 0xFF) ;
        utflen |= (char) ((buffer[1] >> 16) & 0xFF);
        utflen |= (char) ((buffer[2] >> 8) & 0xFF);
        utflen |= (char) ((buffer[3] >> 0) & 0xFF);

        p<string> text = new string( buffer + 4, this->content.size() - 4 ) ;
        return text ;
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
        int length = (int)strlen(text) ;
        int utflen = length ;

        // TODO: assuming that the text is ASCII
        this->content = array<char> (length + 4) ;

        this->content[0] = (char) ((utflen >> 24) & 0xFF) ;
        this->content[1] = (char) ((utflen >> 16) & 0xFF);
        this->content[2] = (char) ((utflen >> 8) & 0xFF);
        this->content[3] = (char) ((utflen >> 0) & 0xFF);

        for( int i = 0 ; i < length ; i++ )
            this->content[4+i] = text[i] ;
    }
    else
        this->content = NULL ;
}
