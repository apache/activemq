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
#include "command/ActiveMQMapMessage.hpp"

using namespace apache::activemq::client::command;

/*
 * 
 */
ActiveMQMapMessage::ActiveMQMapMessage()
{
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
int ActiveMQMapMessage::getCommandType()
{
    return ActiveMQMapMessage::TYPE ;
}

/*
 * 
 */
bool ActiveMQMapMessage::getBoolean(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No boolean value available for given key") ;

    try
    {
        // Try to get value as a boolean
        return tempIter->second->getBoolean() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
char ActiveMQMapMessage::getByte(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No byte value available for given key") ;

    try
    {
        // Try to get value as a byte
        return tempIter->second->getByte() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
ap<char> ActiveMQMapMessage::getBytes(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No byte array value available for given key") ;

    try
    {
        // Try to get value as a byte array
        return tempIter->second->getBytes() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setBytes(const char* name, ap<char> value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
double ActiveMQMapMessage::getDouble(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No double value available for given key") ;

    try
    {
        // Try to get value as a double
        return tempIter->second->getDouble() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
float ActiveMQMapMessage::getFloat(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No float value available for given key") ;

    try
    {
        // Try to get value as a float
        return tempIter->second->getFloat() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setFloat(const char* name, float value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
int ActiveMQMapMessage::getInt(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No integer value available for given key") ;

    try
    {
        // Try to get value as an integer
        return tempIter->second->getInt() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
long long ActiveMQMapMessage::getLong(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No long value available for given key") ;

    try
    {
        // Try to get value as a long
        return tempIter->second->getLong() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
short ActiveMQMapMessage::getShort(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No short value available for given key") ;

    try
    {
        // Try to get value as a short
        return tempIter->second->getShort() ;
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

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
p<string> ActiveMQMapMessage::getString(const char* name) throw (MessageFormatException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    if( tempIter == contentMap.end() )
        throw MessageFormatException("No short value available for given key") ;

    try
    {
        // Try to get value as a string
        return tempIter->second->getString() ;
    }
    catch( ConversionException ce )
    {
        throw MessageFormatException( ce.what() ) ;
    }
}

/*
 * 
 */
void ActiveMQMapMessage::setString(const char* name, p<string> value) throw (IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || strcmp(name, "") == 0 )
        throw IllegalArgumentException("Invalid key name") ;

    p<string> key = new string(name) ;
    contentMap[key] = new MapItemHolder(value) ;
}

/*
 * 
 */
ap<string> ActiveMQMapMessage::getMapNames()
{
    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    ap<string> keyNames (contentMap.size()) ;
    int        index = 0 ;

    for( tempIter = contentMap.begin() ;
         tempIter != contentMap.end() ;
         tempIter++ )
    {
        keyNames[index++] = tempIter->first ;
    }
    return keyNames ;
}

/*
 * 
 */
bool ActiveMQMapMessage::itemExists(const char* name)
{
    map< p<string>, p<MapItemHolder> >::const_iterator tempIter ;
    p<string> key = new string(name) ;

    // Check if key exists in map
    tempIter = contentMap.find(key) ;
    return ( tempIter != contentMap.end() ) ? true : false ;
}
