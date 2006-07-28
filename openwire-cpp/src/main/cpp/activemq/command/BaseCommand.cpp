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
#include "activemq/command/BaseCommand.hpp"

using namespace apache::activemq::command;


// Attribute methods ------------------------------------------------

int BaseCommand::getHashCode()
{
    return ( commandId * 38 ) + getDataStructureType() ;
}


// Operation methods ------------------------------------------------

bool BaseCommand::operator== (BaseCommand& that)
{
    if( this->getDataStructureType() == that.getDataStructureType() &&
        this->commandId == that.commandId )
    {
        return true ;
    }
    return false ;
}

p<string> BaseCommand::toString()
{
    p<string> str = new string() ;
    char      buffer[10] ;
    
    str->assign( getDataStructureTypeAsString( getDataStructureType() )->c_str() ) ;

    if( str->length() == 0 )
        str->assign("") ;

    str->append(": id = ") ;
	sprintf(buffer, "%d", commandId) ;
    str->append( buffer ) ;

    return str ;
}
