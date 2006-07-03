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
#include "Guid.h"
#include <stdexcept>

using namespace activemq::util;
using namespace activemq::exceptions;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
Guid::Guid(void)
{
   // Clear internal uuid, would pass isNull
   #if defined( unix ) && !defined( __CYGWIN__ )
      memset(&uuid, 0, sizeof(uuid_t));
   #else
      ::UuidCreateNil(&uuid);
   #endif
}

////////////////////////////////////////////////////////////////////////////////
Guid::Guid(const Guid& source)
{
   // Set this uuid to that of the source
   *this = source;
}

////////////////////////////////////////////////////////////////////////////////
Guid::Guid(const std::string& source)
   throw ( IllegalArgumentException )
{
   if(source == "")
   {
      throw IllegalArgumentException(
         __FILE__, __LINE__,
         "GUID::fromBytes - Source was Empty");
   }

   // Set this uuid to that of the source
   *this = source;   
}
   
////////////////////////////////////////////////////////////////////////////////
Guid::~Guid(void)
{
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::isNull(void) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // Check the uuid APIs is null method
      return uuid_is_null(*(const_cast<uuid_t*>(&uuid))) == 1 ? true : false;
   #else
	   RPC_STATUS status;

      BOOL result = ::UuidIsNil( const_cast<GUID*>( &uuid ), &status );

	   return (result == TRUE) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
void Guid::setNull(void)
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // use the uuid function to clear
      uuid_clear(uuid);
   #else
      ::UuidCreateNil(&uuid);
   #endif
}

////////////////////////////////////////////////////////////////////////////////
Guid& Guid::createGUID(void) throw( RuntimeException )
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // Use the uuid_generate method to create a new GUID
      uuid_generate(uuid);
   #else
	   // Create a uuid with the Co Create GUID
	   RPC_STATUS lhResult = ::UuidCreate( &uuid );

	   if ( lhResult == RPC_S_UUID_NO_ADDRESS )
	   {
         throw RuntimeException(
            __FILE__, __LINE__,
            "GUIG::createGUID - Failed Creating GUID");
	   }
   #endif

   return *this;
}

////////////////////////////////////////////////////////////////////////////////
std::string Guid::toString(void) const throw( RuntimeException )
{
   std::string uuid_str = "";

   #if defined( unix ) && !defined( __CYGWIN__ )
      // Create storage for the string buffer
      char buffer[36] = {0};
      
      // parse the uuid to the string
      uuid_unparse(*(const_cast<uuid_t*>(&uuid)), buffer);
      
      // Store it in a string
      uuid_str = buffer;
   #else   
	   // Convert the GUID object to a string.
	   unsigned char* guidStr = 0;

	   RPC_STATUS result = ::UuidToString(
         const_cast<GUID*>(&uuid),
		   &guidStr);

	   if(result == RPC_S_OUT_OF_MEMORY)
	   {
         throw RuntimeException(
            __FILE__, __LINE__, 
            "GUIG::createGUID - Failed Creating GUID");
	   }

	   uuid_str = (char*)guidStr;

	   // Dispose of the GUID string.
	   ::RpcStringFree(&guidStr);
   #endif

   return uuid_str;
}

////////////////////////////////////////////////////////////////////////////////
Guid::operator std::string() const
{
   return toString();
}

////////////////////////////////////////////////////////////////////////////////
const unsigned char* Guid::toBytes(void) const
{
   unsigned char* buffer = new unsigned char[getRawBytesSize()];
   
   // copy our buffer
   #if defined( unix ) && !defined( __CYGWIN__ )
      uuid_copy(buffer, *(const_cast<uuid_t*>(&uuid)));
   #else
      memcpy(buffer, &uuid, getRawBytesSize());
   #endif
   
   return &buffer[0]; 
}

////////////////////////////////////////////////////////////////////////////////
Guid& Guid::fromBytes(const unsigned char* bytes) 
   throw ( IllegalArgumentException )
{
   if(bytes == NULL)
   {
      throw IllegalArgumentException(
         __FILE__, __LINE__,
         "GUID::fromBytes - bytes pointer was NULL");
   }
   
   // Copy the data
   #if defined( unix ) && !defined( __CYGWIN__ )
      memcpy(uuid, bytes, getRawBytesSize());
   #else
      memcpy(&uuid, bytes, getRawBytesSize());
   #endif

   return *this;
}

////////////////////////////////////////////////////////////////////////////////
int Guid::getRawBytesSize(void) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      return sizeof(uuid_t);
   #else
      return sizeof(::GUID);
   #endif
}

////////////////////////////////////////////////////////////////////////////////
Guid::operator const unsigned char*() const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      return &uuid[0];
   #else
      return reinterpret_cast<const unsigned char*>(&uuid);
   #endif
}

////////////////////////////////////////////////////////////////////////////////
Guid& Guid::operator=(const Guid& source)
   throw ( IllegalArgumentException )
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // Use the uuid method to copy
      uuid_copy(uuid, *(const_cast<uuid_t*>(&source.uuid)));
   #else
      // Use mem copy
      memcpy(&uuid, &source.uuid, getRawBytesSize());
   #endif

   return *this;
}

////////////////////////////////////////////////////////////////////////////////
Guid& Guid::operator=(const std::string& source) 
   throw ( IllegalArgumentException )
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // Parse a uuid from the passed in string
      uuid_parse( const_cast<char*>(source.c_str()), uuid );
   #else
	   if ( source.empty() )
	   {
		   ::UuidCreateNil( &uuid );
	   }
	   else
	   {
		   RPC_STATUS hResult =
			   ::UuidFromString( (unsigned char*)source.c_str(), &uuid );

		   if ( hResult == RPC_S_INVALID_STRING_UUID )
		   {
            throw IllegalArgumentException(
               __FILE__, __LINE__,
               "GUID::fromBytes - Invalid GUID String");
		   }
	   }
   #endif

   return *this;
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator==(const Guid& source) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // uuid_compare returns 0 for equal
      return uuid_compare(
               *(const_cast<uuid_t*>(&uuid)), 
               *(const_cast<uuid_t*>(&source.uuid))) == 0 ? true : false;
   #else
	   RPC_STATUS status;

	   BOOL result = ::UuidEqual(
         const_cast<GUID*>( &uuid ),
         const_cast<GUID*>( &source.uuid ),
		   &status );

	   return ( result == TRUE ) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator==(const std::string& source) const
{
   return *this == Guid(source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator!=(const Guid& source) const
{
   return !(*this == source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator!=(const std::string& source) const
{
   return !(*this == source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator<(const Guid& source) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // uuid_compare returns 0 for equal
      return uuid_compare(
               *(const_cast<uuid_t*>(&uuid)), 
               *(const_cast<uuid_t*>(&source.uuid))) < 0 ? true : false;
   #else
      RPC_STATUS status;

      int result = ::UuidCompare(
         const_cast<GUID*>( &uuid ),
         const_cast<GUID*>( &source.uuid ),
		   &status );

	   return ( result < 0 ) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator<(const std::string& source) const
{
   return *this < Guid(source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator<=(const Guid& source) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // uuid_compare returns 0 for equal
      return uuid_compare(
               *(const_cast<uuid_t*>(&uuid)), 
               *(const_cast<uuid_t*>(&source.uuid))) <= 0 ? true : false;
   #else
      RPC_STATUS status;

      int result = ::UuidCompare(
         const_cast<GUID*>( &uuid ),
         const_cast<GUID*>( &source.uuid ),
		   &status );

	   return ( result <= 0 ) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator<=(const std::string& source) const
{
   return *this <= Guid(source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator>(const Guid& source) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // uuid_compare returns 0 for equal
      return uuid_compare(
               *(const_cast<uuid_t*>(&uuid)), 
               *(const_cast<uuid_t*>(&source.uuid))) > 0 ? true : false;
   #else
      RPC_STATUS status;

      int result = ::UuidCompare(
         const_cast<GUID*>( &uuid ),
         const_cast<GUID*>( &source.uuid ),
		   &status );

	   return ( result > 0 ) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator>(const std::string& source) const
{
   return *this > Guid(source);
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator>=(const Guid& source) const
{
   #if defined( unix ) && !defined( __CYGWIN__ )
      // uuid_compare returns 0 for equal
      return uuid_compare(
               *(const_cast<uuid_t*>(&uuid)), 
               *(const_cast<uuid_t*>(&source.uuid))) >= 0 ? true : false;
   #else
      RPC_STATUS status;

      int result = ::UuidCompare(
         const_cast<GUID*>(&uuid),
         const_cast<GUID*>(&source.uuid),
		   &status);

	   return (result >= 0) ? true : false;
   #endif
}

////////////////////////////////////////////////////////////////////////////////
bool Guid::operator>=(const std::string& source) const
{
   return *this >= Guid(source);
}

////////////////////////////////////////////////////////////////////////////////
std::string Guid::createGUIDString(void)
{
   return Guid().createGUID().toString();
}

////////////////////////////////////////////////////////////////////////////////
const unsigned char* createGUIDBytes(void)
{
   return Guid().createGUID().toBytes();
}
