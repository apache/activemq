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
#ifndef ACTIVEMQ_UTIL_GUID_H
#define ACTIVEMQ_UTIL_GUID_H

#if defined( unix ) || defined(__APPLE__) && !defined( __CYGWIN__ ) 
    #include <uuid/uuid.h>
#elif defined(_WIN32) || defined( __CYGWIN__ )
    #include <objbase.h>
    #include <rpcdce.h>
#else // defined MACOSX
    #include "uuid.h"
#endif

#include <activemq/exceptions/RuntimeException.h>
#include <activemq/exceptions/IllegalArgumentException.h>

#include <string>

namespace activemq{
namespace util{
   
    class Guid
    {
    public:

        Guid(void);
        Guid(const Guid& source);
        Guid(const std::string& source)
            throw ( exceptions::IllegalArgumentException );
        virtual ~Guid(void);

        /**
         * Determines if this GUID is null, if so it can be initialized with a 
         * call to <code>createGUID</code>.
         * @return true for Null GUID, false otherwise.
         */
        bool isNull(void) const;
      
        /**
         * Clears the GUID's current value and sets it to a NULL GUID value
         * will now pass <code>isNull</code>.
         */
        void setNull(void);

        /**
         * Generate a new GUID which will overwrite any current GUID value
         * @return Reference to this object that now has a new GUID
         */       
        Guid& createGUID(void) throw( exceptions::RuntimeException );

        /** 
         * Converts the GUID to a string and returns that string
         * @return a string with this GUID's stringified value
         */
        std::string toString(void) const throw( exceptions::RuntimeException );
      
        /** 
         * Converts the GUID to a byte array and return a pointer to the
         * new array, called takes ownership and must delete this array
         * when done.  
         * @return a byte array with the GUID byte value, size = 16
         */
        const unsigned char* toBytes(void) const;
      
        /**
         * Initializes this GUID with the GUID specified in the bytes parameter
         * @return reference to this object.
         */
        Guid& fromBytes( const unsigned char* bytes )    
            throw ( exceptions::IllegalArgumentException );
      
        /**
         * Returns the Size in Bytes of the Raw bytes representation of the
         * GUID.
         * @return size of the Raw bytes representation
         */
        int getRawBytesSize(void) const;

        /**
         * string type cast operator
         * @returns string representation of this GUID
         */
        operator std::string() const;
      
        /**
         * byte array cast operator, caller does not own this memeory
         * @returns byte array with the GUID byte value representation
         */
        operator const unsigned char*() const;
      
        /**
         * Assignment operators
         * @return Reference to this GUID object
         */
        Guid& operator=( const Guid& source )          
           throw ( exceptions::IllegalArgumentException );
        Guid& operator=( const std::string& source )
           throw ( exceptions::IllegalArgumentException );

        /**
         * Equality Comparison Operators
         * @return true for equal. false otherwise
         */
        bool operator==( const Guid& source ) const;
        bool operator==( const std::string& source ) const;
      
        /**
         * Inequality Comparison Operators
         * @return true for equal. false otherwise
         */
        bool operator!=( const Guid& source ) const;
        bool operator!=( const std::string& source ) const;
      
        /**
         * Less than operators
         * @return true for equal. false otherwise
         */
        bool operator<(const Guid& source) const;
        bool operator<(const std::string& source) const;
      
        /**
         * Less than or equal to operators
         * @return true for equal. false otherwise
         */
        bool operator<=( const Guid& source ) const;
        bool operator<=( const std::string& source ) const;

        /**
         * Greater than operators
         * @return true for equal. false otherwise
         */
        bool operator>( const Guid& source ) const;
        bool operator>( const std::string& source ) const;

        /**
         * Greater than or equal to operators
         * @return true for equal. false otherwise
         */
        bool operator>=( const Guid& source ) const;
        bool operator>=( const std::string& source ) const;
      
    public:
   
        /**
         * Static Guid Creation Method, creates a GUID and returns it as a string
         * @return Guid string.
         */
        static std::string createGUIDString(void);
      
        /**
         * Static Guid Create Method, create a GUID and returns the byte representation
         * of the new GUID.
         * @return Guid bytes array, size is 16
         */
        static const unsigned char* createGUIDBytes(void);
   
    private:

        // the uuid that this object represents.
        #if defined( unix ) || defined(__APPLE__)
            uuid_t uuid;
        #else
            ::GUID uuid;
        #endif

   };

}}

#endif /*ACTIVEMQ_UTIL_GUID_H*/
