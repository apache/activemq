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
#include "activemq/command/BaseCommand.hpp"

using namespace apache::activemq::command;


// Attribute methods ------------------------------------------------

/*
 * 
 */
int BaseCommand::getCommandId()
{
    return commandId ; 
}

/*
 * 
 */
void BaseCommand::setCommandId(int id)
{
    commandId = id ; 
}

/*
 * 
 */
bool BaseCommand::getResponseRequired()
{
    return responseRequired ;
}

/*
 * 
 */
void BaseCommand::setResponseRequired(bool value)
{
    responseRequired = value ;
}

/*
 * 
 */
int BaseCommand::getHashCode()
{
    return ( commandId * 38 ) + BaseDataStructure::getDataStructureType() ;
}


// Operation methods ------------------------------------------------

/*
 * 
 */
int BaseCommand::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw(IOException)
{
    int size = 0 ;

    size += marshaller->marshalInt(commandId, mode, ostream) ;
    size += marshaller->marshalBoolean(responseRequired, mode, ostream) ; 

    return size ;
}

/*
 * 
 */
void BaseCommand::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw(IOException)
{
    commandId = marshaller->unmarshalInt(mode, istream) ;
    responseRequired = marshaller->unmarshalBoolean(mode, istream) ; 
}

/*
 * 
 */
bool BaseCommand::operator== (BaseCommand& that)
{
    if( BaseDataStructure::getDataStructureType() == ((BaseDataStructure)that).getDataStructureType() &&
        this->commandId == that.commandId )
    {
        return true ;
    }
    return false ;
}

/*
 * 
 */
p<string> BaseCommand::toString()
{
    p<string> str = new string() ;
    char      buffer[10] ;
    
    str->assign( BaseDataStructure::getDataStructureTypeAsString( BaseDataStructure::getDataStructureType() )->c_str() ) ;

    if( str->length() == 0 )
        str->assign("") ;

    str->append(": id = ") ;
	sprintf(buffer, "%d", commandId) ;
    str->append( buffer ) ;

    return str ;
}
