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
#include "command/BaseCommand.hpp"

using namespace apache::activemq::client::command;

/*
 * 
 */
BaseCommand::BaseCommand()
{
}

BaseCommand::~BaseCommand()
{
}


// Attribute methods ------------------------------------------------

int BaseCommand::getHashCode()
{
    return commandId ;
}

int BaseCommand::getCommandType()
{
    return BaseCommand::TYPE ;
}

int BaseCommand::getCommandId()
{
    return commandId ;
}

void BaseCommand::setCommandId(int id)
{
    commandId = id ;
}

bool BaseCommand::isResponseRequired()
{
    return responseRequired ;
}

void BaseCommand::setResponseRequired(bool required)
{
    responseRequired = required ;
}


// Operation methods ------------------------------------------------

bool BaseCommand::operator== (BaseCommand& other)
{
    if( getCommandType() == other.getCommandType() &&
        commandId == other.commandId &&
        responseRequired == other.responseRequired )
    {
        return true ;
    }
    return false ;
}

p<string> BaseCommand::toString()
{
    p<string> str = new string() ;
    char      buffer[5] ;
    
    str->assign( getCommandTypeAsString( getCommandType() )->c_str() ) ;

    if( str->length() == 0 )
        str->assign("") ;

    str->append(": id = ") ;
    
    sprintf( buffer, "%d", getCommandId() );
    str->append( buffer ) ;

    return str ;
}
