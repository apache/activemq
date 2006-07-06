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
#include "activemq/command/ActiveMQMapMessage.hpp"

#include "ppr/io/ByteArrayOutputStream.hpp"
#include "ppr/io/ByteArrayInputStream.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/DataInputStream.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
ActiveMQMapMessage::ActiveMQMapMessage()
{
    contentMap = new PropertyMap() ;
}

/*
 * 
 */
ActiveMQMapMessage::~ActiveMQMapMessage()
{
}

/*
 * 
 */
unsigned char ActiveMQMapMessage::getDataStructureType()
{
    return ActiveMQMapMessage::TYPE ;
}

/*
 * 
 */
p<PropertyMap> ActiveMQMapMessage::getBody()
{
    return contentMap ;
}

/*
 * 
 */
bool ActiveMQMapMessage::getBoolean(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = string(name) ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No boolean value available for given key") ;

    try
    {
        // Try to get value as a boolean
        return tempIter->second.getBoolean() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setBoolean(const char* name, bool value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    //contentMap->insert (pair<string,p<MapItemHolder> > (key, new MapItemHolder(value)));
    (*contentMap)[key] = MapItemHolder(value);
}

/*
 * 
 */
char ActiveMQMapMessage::getByte(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = string(name) ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No byte value available for given key") ;

    try
    {
        // Try to get value as a byte
        return tempIter->second.getByte() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setByte(const char* name, char value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value);
}

/*
 * 
 */
array<char> ActiveMQMapMessage::getBytes(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = string(name) ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No byte array value available for given key") ;

    try
    {
        // Try to get value as a byte array
        return tempIter->second.getBytes() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setBytes(const char* name, array<char> value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value);
}

/*
 * 
 */
double ActiveMQMapMessage::getDouble(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No double value available for given key") ;

    try
    {
        // Try to get value as a double
        return tempIter->second.getDouble() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setDouble(const char* name, double value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
float ActiveMQMapMessage::getFloat(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No float value available for given key") ;

    try
    {
        // Try to get value as a float
        return tempIter->second.getFloat() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*F
 * 
 */
void ActiveMQMapMessage::setFloat(const char* name, float value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
int ActiveMQMapMessage::getInt(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No integer value available for given key") ;

    try
    {
        // Try to get value as an integer
        return tempIter->second.getInt() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setInt(const char* name, int value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
long long ActiveMQMapMessage::getLong(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No long value available for given key") ;

    try
    {
        // Try to get value as a long
        return tempIter->second.getLong() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setLong(const char* name, long long value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
short ActiveMQMapMessage::getShort(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No short value available for given key") ;

    try
    {
        // Try to get value as a short
        return tempIter->second.getShort() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setShort(const char* name, short value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
p<string> ActiveMQMapMessage::getString(const char* name) throw (MessageFormatException, IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    if( tempIter == contentMap->end() )
        throw MessageFormatException("No short value available for given key") ;

    try
    {
        // Try to get value as a string
        return tempIter->second.getString() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setString(const char* name, const char* value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    string key = name ;
    (*contentMap)[key] = MapItemHolder(value) ;
}

/*
 * 
 */
array<string> ActiveMQMapMessage::getMapNames()
{
    PropertyMap::iterator tempIter ;
    array<string> keyNames (contentMap->size()) ;
    int        index = 0 ;

    for( tempIter = contentMap->begin() ;
         tempIter != contentMap->end() ;
         tempIter++ )
    {
        keyNames[index++] = new string(tempIter->first) ;
    }
    return keyNames ;
}

/*
 * 
 */
bool ActiveMQMapMessage::itemExists(const char* name)
{
    PropertyMap::iterator tempIter ;
    string key = name ;

    // Check if key exists in map
    tempIter = contentMap->find(key) ;
    return ( tempIter != contentMap->end() ) ? true : false ;
}

/*
 *
 */
int ActiveMQMapMessage::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw (IOException)
{
    int size = 0 ;

    // Update message content during size lookup
    if( mode == IMarshaller::MARSHAL_SIZE )
    {
        p<ByteArrayOutputStream> bos = new ByteArrayOutputStream() ;
        p<DataOutputStream>      dos = new DataOutputStream( bos ) ;

        // Marshal map into a byte array
        marshaller->marshalMap(contentMap, IMarshaller::MARSHAL_WRITE, dos) ;

        // Store map byte array in message content
        this->content = bos->toArray() ;
    }
    // Note! Message content marshalling is done in super class
    size += ActiveMQMessage::marshal(marshaller, mode, ostream) ;

    return size ;
}

/*
 *
 */
void ActiveMQMapMessage::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw (IOException)
{
    // Note! Message content unmarshalling is done in super class
    ActiveMQMessage::unmarshal(marshaller, mode, istream) ;

    // Extract map from message content
    if( mode == IMarshaller::MARSHAL_READ )
    {
        p<ByteArrayInputStream> bis = new ByteArrayInputStream( this->content ) ;
        p<DataInputStream>      dis = new DataInputStream( bis ) ;

        // Unmarshal map into a map
        contentMap = marshaller->unmarshalMap(mode, dis) ;
    }
}
