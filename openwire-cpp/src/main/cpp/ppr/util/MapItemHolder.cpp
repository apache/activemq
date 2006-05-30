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
#include "ppr/util/MapItemHolder.hpp"

using namespace apache::ppr::util;

#if defined (_MSC_VER) // Microsoft Visual Studio compiler
#include <windows.h> // Defines LONG_PTR which is a long type with same size as a void*
#else
typedef long LONG_PTR; // Assume sizeof(long) == sizeof(void*)
#endif


// Constructors -----------------------------------------------------

/*
 * 
 */
MapItemHolder::MapItemHolder()
{
    this->value = NULL ;
    this->type  = MapItemHolder::UNDEFINED ;
    this->flags = 0 ;
}

MapItemHolder::MapItemHolder(const MapItemHolder& other) :
    type (other.type),
    flags (other.flags)
{
    if (flags & BIT_DESTRUCT) {
        // value is a ptr
        if ((flags & BIT_RELEASE_P_REFCOUNTED) == BIT_RELEASE_P_REFCOUNTED) {
            // value is a refcounted ptr
            this->value = other.value;
            if (value) addref (reinterpret_cast<refcounted*> (value));
        } else {
            // value is a standard ptr - handle specificly for each type:
            switch (type) {
              case DOUBLE:
                this->value = new double (other.getDouble());
                break;
              case LONG:
                this->value = new long long (other.getLong());
                break;
              default:
                assert (false); // Unknown type that requires heap allocation...
            }
        }
    } else {
        this->value = other.value;
    }
}

MapItemHolder&
MapItemHolder::operator = (const MapItemHolder& other)
{
    // Destruct us:
    this->~MapItemHolder();

    // Constuct us identical to how our copy constructor does:
    this->type = other.type;
    this->flags = other.flags;

    if (flags & BIT_DESTRUCT) {
        // value is a ptr
        if ((flags & BIT_RELEASE_P_REFCOUNTED) == BIT_RELEASE_P_REFCOUNTED) {
            // value is a refcounted ptr
            this->value = other.value;
            if (value) addref (reinterpret_cast<refcounted*> (value));
        } else {
            // value is a standard ptr - handle specificly for each type:
            switch (type) {
              case DOUBLE:
                this->value = new double (other.getDouble());
                break;
              case LONG:
                this->value = new long long (other.getLong());
                break;
              default:
                assert (false); // Unknown type that requires heap allocation...
            }
        }
    } else {
        this->value = other.value;
    }
    return *this;
}


/*
 * 
 */
MapItemHolder::MapItemHolder(bool value)
{
    this->value = (value ? (void*) 1 : NULL);
    this->type  = MapItemHolder::BOOLEAN ;
    this->flags = 0 ;
}

/*
 * 
 */
MapItemHolder::MapItemHolder(char value)
{
    this->value = (void*) (LONG_PTR) value;
    this->type  = MapItemHolder::BYTE ;
    this->flags = 0 ;
}

/*
 * 
 */
MapItemHolder::MapItemHolder(array<char> value)
{
    this->value = static_cast<refcounted*> (getptr (array_help::get_array (value)));
    if (value != NULL) addref(value) ;
    this->type  = MapItemHolder::BYTEARRAY ;
    this->flags = 
        MapItemHolder::BIT_RELEASE_P_REFCOUNTED; // Tells destructor to release value
}

/*
 * 
 */
MapItemHolder::MapItemHolder(double value)
{
    this->value = new double(value) ;
    this->type  = MapItemHolder::DOUBLE ;
    this->flags = 
        MapItemHolder::BIT_DESTRUCT; // Tells destructor to delete value
}

/*
 * 
 */
MapItemHolder::MapItemHolder(float value)
{
    this->value = (void*) (LONG_PTR) value;
    this->type  = MapItemHolder::FLOAT ;
    this->flags = 0 ;
}

/*
 * 
 */
MapItemHolder::MapItemHolder(int value)
{
    this->value = (void*) (LONG_PTR) value;
    this->type  = MapItemHolder::INTEGER ;
    this->flags = 0 ;
}

/*
 * 
 */
MapItemHolder::MapItemHolder(long long value)
{
    this->value = new long long(value) ;
    this->type  = MapItemHolder::LONG ;
    this->flags = 
        MapItemHolder::BIT_DESTRUCT; // Tells destructor to delete value
}

/*
 * 
 */
MapItemHolder::MapItemHolder(short value)
{
    this->value = (void*) (LONG_PTR) value;
    this->type  = MapItemHolder::SHORT ;
    this->flags = 0 ;
}

/*
 * 
 */
MapItemHolder::MapItemHolder(p<string> value)
{
    this->value = p_help::get_rc (value);
    if (value != NULL) addref(value) ;
    this->type  = MapItemHolder::STRING ;
    this->flags = 
        MapItemHolder::BIT_RELEASE_P_REFCOUNTED; // Tells destructor to release value
}

/*
 * 
 */
MapItemHolder::MapItemHolder(const char* value)
{
    p<string> val = new string (value);
    this->value = p_help::get_rc (val);
    addref(val) ;
    this->type  = MapItemHolder::STRING ;
    this->flags = 
        MapItemHolder::BIT_RELEASE_P_REFCOUNTED; // Tells destructor to release value
}

/*
 * 
 */
MapItemHolder::~MapItemHolder()
{
    if (flags & BIT_DESTRUCT) {
        // value must be destructed or released
        if ((flags & BIT_RELEASE_P_REFCOUNTED) == BIT_RELEASE_P_REFCOUNTED) {
            // value is a p<refcounted> - release it
            if (value) release (reinterpret_cast<refcounted*> (value));
        } else {
            // value is a primitive type allocated with new() - delete it.
            delete (int*) value ;
        }
    }
}


// Attribute methods ------------------------------------------------

/*
 * 
 */
int MapItemHolder::getType() const 
{
    return type ;
}

/*
 * 
 */
bool MapItemHolder::getBoolean() const throw (ConversionException)
{
    if( type == BOOLEAN )
        return ((LONG_PTR) value ? true : false);
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a boolean") ;
}

/*
 * 
 */
char MapItemHolder::getByte() const throw (ConversionException)
{
    if( type == BYTE )
        return (char) (LONG_PTR) value;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a byte") ;
}

/*
 * 
 */
array<char> MapItemHolder::getBytes() const throw (ConversionException)
{
    if( type == BYTEARRAY ) {
        array<char> retval;
        array_help::get_array (retval) = smartify (
            static_cast<RCArray<char>*> (
                reinterpret_cast<refcounted*> (value)));
        if (retval != NULL) addref (retval);
        return retval;
    }
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a byte array") ;
}

/*
 * 
 */
double MapItemHolder::getDouble() const throw (ConversionException)
{
    if( type == DOUBLE ) {
        return *(reinterpret_cast<double*> (value)) ;
    } else if ( type == FLOAT ) {
        return (double) (float) (LONG_PTR) value;
    } else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a double") ;
}

/*
 * 
 */
float MapItemHolder::getFloat() const throw (ConversionException)
{
    if( type == FLOAT )
        return (float) (LONG_PTR) value;
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a float") ;
}

/*
 * 
 */
int MapItemHolder::getInt() const throw (ConversionException)
{
    if( type & (INTEGER | SHORT | BYTE)) {
        return (int) (LONG_PTR) value;
    }
    else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to an integer") ;
}

/*
 * 
 */
long long MapItemHolder::getLong() const throw (ConversionException)
{
    if( type & (INTEGER | SHORT | BYTE)) {
        return (long long) (LONG_PTR) value;
    } else if ( type == LONG ) {
        return *(reinterpret_cast<long long*> (value)) ;
    } else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a long") ;
}

/*
 * 
 */
short MapItemHolder::getShort() const throw (ConversionException)
{
    if( type & (SHORT | BYTE)) {
        return (short) (LONG_PTR) value;
    } else
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a short") ;
}

/*
 * 
 */
p<string> MapItemHolder::getString() const throw (ConversionException)
{
    p<string> retval ;
    char buffer[80] ;

    if (type == STRING) {
        p_help::set_rc (retval, value);
        p_help::set_obj (retval, static_cast<void_refcounter<string>*>(value)->obj_);
        if (retval != NULL) addref (retval);
        return retval;
    } else if (type == BOOLEAN) {
        sprintf(buffer, "%s", ( getBoolean() ? "true" : "false" ) ) ;
        retval = new string(buffer) ;
        return retval ;
    } else if (type & (BYTE | SHORT | INTEGER)) {
        sprintf(buffer, "%d", getInt() ) ;
        retval = new string(buffer) ;
        return retval ;
    } else if (type == LONG) {
#ifdef unix
        sprintf(buffer, "%lld", getLong() ) ;
#else
        sprintf(buffer, "%I64d", getLong() ) ;
#endif
        retval = new string(buffer) ;
        return retval ;
    } else if (type == FLOAT) {
        sprintf(buffer, "%f", getFloat() ) ;
        retval = new string(buffer) ;
        return retval ;
    } else if (type == DOUBLE) {
        sprintf(buffer, "%lf", getDouble() ) ;
        retval = new string(buffer) ;
        return retval ;
    } else {
        throw ConversionException(__FILE__, __LINE__, "Cannot convert map item value to a string") ;
    }
}
