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
#include "util/MapItemHolder.hpp"

using namespace apache::activemq::client::util;

/*
 * 
 */
MapItemHolder::MapItemHolder(bool value)
{
    this->value = new bool(value) ;
    this->type  = MapItemHolder::BOOLEAN ;
}

MapItemHolder::MapItemHolder(char value)
{
    this->value = new char(value) ;
    this->type  = MapItemHolder::BYTE ;
}

MapItemHolder::MapItemHolder(ap<char> value)
{
    this->value = ap_help::get_obj (value);
    addref(value) ;
    this->type  = MapItemHolder::BYTEARRAY ;
}

MapItemHolder::MapItemHolder(double value)
{
    this->value = new double(value) ;
    this->type  = MapItemHolder::DOUBLE ;
}

MapItemHolder::MapItemHolder(float value)
{
    this->value = new float(value) ;
    this->type  = MapItemHolder::FLOAT ;
}

MapItemHolder::MapItemHolder(int value)
{
    this->value = new int(value) ;
    this->type  = MapItemHolder::INTEGER ;
}

MapItemHolder::MapItemHolder(long long value)
{
    this->value = new long long(value) ;
    this->type  = MapItemHolder::LONG ;
}

MapItemHolder::MapItemHolder(short value)
{
    this->value = new short(value) ;
    this->type  = MapItemHolder::SHORT ;
}

MapItemHolder::MapItemHolder(p<string> value)
{
    this->value = p_help::get_obj (value);
    addref(value) ;
    this->type  = MapItemHolder::STRING ;
}


MapItemHolder::~MapItemHolder()
{
    if( type == BYTEARRAY )
        release (reinterpret_cast<refcounted*> (value));
    else if( type == STRING )
        reinterpret_cast<PointerHolder<string>*> (value)->release();
    else
        delete value ;
}

bool MapItemHolder::getBoolean() throw (ConversionException)
{
    if( type == BOOLEAN )
        return *(reinterpret_cast<bool*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a boolean") ;
}

char MapItemHolder::getByte() throw (ConversionException)
{
    if( type == BYTE )
        return *(reinterpret_cast<char*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a byte") ;
}

ap<char> MapItemHolder::getBytes() throw (ConversionException)
{
    if( type == BYTEARRAY ) {
        ap<char> retval;
        ap_help::set_obj (retval, value);
        addref (retval);
        return retval;
    }
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a byte array") ;
}

double MapItemHolder::getDouble() throw (ConversionException)
{
    if( type == DOUBLE || type == FLOAT )
        return *(reinterpret_cast<double*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a double") ;
}

float MapItemHolder::getFloat() throw (ConversionException)
{
    if( type == FLOAT )
        return *(reinterpret_cast<float*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a float") ;
}

int MapItemHolder::getInt() throw (ConversionException)
{
    if( type == INTEGER || type == SHORT || type == LONG )
        return *(reinterpret_cast<int*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to an integer") ;
}

long long MapItemHolder::getLong() throw (ConversionException)
{
    if( type == INTEGER || type == SHORT || type == LONG )
        return *(reinterpret_cast<long long*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a long") ;
}

short MapItemHolder::getShort() throw (ConversionException)
{
    if( type == INTEGER || type == SHORT || type == LONG )
        return *(reinterpret_cast<short*> (value)) ;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a short") ;
}

p<string> MapItemHolder::getString() throw (ConversionException)
{
    p<string> retval ;
    char buffer[25] ;

    switch( type )
    {
        case STRING:
            p_help::set_obj (retval, value) ;
            addref (retval) ;
            return retval ;
    
        case BOOLEAN:
            sprintf(buffer, "%s", ( getBoolean() ? "true" : "false" ) ) ;
            retval = new string(buffer) ;
            return retval ;

        case BYTE:
        case SHORT:
        case INTEGER:
            sprintf(buffer, "%d", getInt() ) ;
            retval = new string(buffer) ;
            return retval ;

        case LONG:
            sprintf(buffer, "%I64d", getLong() ) ;
            retval = new string(buffer) ;
            return retval ;

        case FLOAT:
            sprintf(buffer, "%f", getFloat() ) ;
            retval = new string(buffer) ;
            return retval ;

        case DOUBLE:
            sprintf(buffer, "%lf", getDouble() ) ;
            retval = new string(buffer) ;
            return retval ;

        default:
            throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a string") ;
    }
}
