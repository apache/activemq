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
#ifndef _CMS_MAPMESSAGE_H_
#define _CMS_MAPMESSAGE_H_

#include <cms/Message.h>

namespace cms
{

    /**
     * A MapMessage object is used to send a set of name-value pairs. The 
     * names are String objects, and the values are primitive data types in 
     * the Java programming language. The names must have a value that is 
     * not null, and not an empty string. The entries can be accessed 
     * sequentially or randomly by name. The order of the entries is 
     * undefined. MapMessage inherits from the Message interface and adds 
     * a message body that contains a Map.
     * 
     * When a client receives a MapMessage, it is in read-only mode. If a 
     * client attempts to write to the message at this point, a 
     * CMSException is thrown.
     */
    class MapMessage : public Message
    {
    public:

        virtual ~MapMessage(void) {}
        
        /**
         * Returns an Enumeration of all the names in the MapMessage 
         * object.
         * @return STL Vector of String values, each of which is the 
         *         name of an item in the MapMessage
         * @throws CMSException
         */
        virtual std::vector< std::string > getMapNames(void) const = 0;

        /**
         * Indicates whether an item exists in this MapMessage object.
         * @param name - String name of the Object in question
         * @return boolean value indicating if the name is in the map
         */
        virtual bool itemExists( const std::string& name ) const = 0;
        
        /**
         * Returns the Boolean value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual bool getBoolean( const std::string& name ) = 0;
        
        /** 
         * Sets a boolean value with the specified name into the Map.
         * @param name - the name of the boolean
         * @param value - the boolean value to set in the Map
         * @throws CMSException
         */
        virtual void setBoolean( const std::string& name,
                                 bool value ) = 0;

        /**
         * Returns the Byte value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual unsigned char getByte( const std::string& name ) = 0;
        
        /** 
         * Sets a Byte value with the specified name into the Map.
         * @param name - the name of the Byte
         * @param value - the Byte value to set in the Map
         * @throws CMSException
         */
        virtual void setByte( const std::string& name,
                              unsigned char value ) = 0;

        /**
         * Returns the Bytes value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual unsigned char* getBytes( const std::string& name ) = 0;
        
        /** 
         * Sets a Bytes value with the specified name into the Map.
         * @param name - the name of the Bytes
         * @param value - the Bytes value to set in the Map
         * @throws CMSException
         */
        virtual void setBytes( const std::string& name,
                               unsigned char* value ) = 0;

        /**
         * Returns the Char value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual char getChar( const std::string& name ) = 0;
        
        /** 
         * Sets a Char value with the specified name into the Map.
         * @param name - the name of the Char
         * @param value - the Char value to set in the Map
         * @throws CMSException
         */
        virtual void setChar( const std::string& name, char value ) = 0;

        /**
         * Returns the Double value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual double getDouble( const std::string& name ) = 0;
        
        /** 
         * Sets a Double value with the specified name into the Map.
         * @param name - the name of the Double
         * @param value - the Double value to set in the Map
         * @throws CMSException
         */
        virtual void setDouble( const std::string& name,
                                double value ) = 0;

        /**
         * Returns the Float value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual float getFloat( const std::string& name ) = 0;
        
        /** 
         * Sets a Float value with the specified name into the Map.
         * @param name - the name of the Float
         * @param value - the Float value to set in the Map
         * @throws CMSException
         */
        virtual void setFloat( const std::string& name, float value ) = 0;

        /**
         * Returns the Int value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual int getInt( const std::string& name ) = 0;
        
        /** 
         * Sets a Int value with the specified name into the Map.
         * @param name - the name of the Int
         * @param value - the Int value to set in the Map
         * @throws CMSException
         */
        virtual void setInt( const std::string& name, int value ) = 0;

        /**
         * Returns the Long value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual long getLong( const std::string& name ) = 0;
        
        /** 
         * Sets a Long value with the specified name into the Map.
         * @param name - the name of the Long
         * @param value - the Long value to set in the Map
         * @throws CMSException
         */
        virtual void setLong( const std::string& name, long value ) = 0;

        /**
         * Returns the Short value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual short getShort( const std::string& name ) = 0;
        
        /** 
         * Sets a Short value with the specified name into the Map.
         * @param name - the name of the Short
         * @param value - the Short value to set in the Map
         * @throws CMSException
         */
        virtual void setShort( const std::string& name, short value ) = 0;

        /**
         * Returns the String value of the Specified name
         * @param name of the value to fetch from the map
         * @throws CMSException
         */
        virtual std::string getString( const std::string& name ) = 0;
        
        /** 
         * Sets a String value with the specified name into the Map.
         * @param name - the name of the String
         * @param value - the String value to set in the Map
         * @throws CMSException
         */
        virtual void setString( const std::string& name, 
                                const std::string& value ) = 0;

    };

}

#endif /*_CMS_MAPMESSAGE_H_*/
