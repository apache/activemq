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
#include "ppr/util/Guid.hpp"

using namespace apache::ppr::util;

Guid::Guid()
{
    // no-op
}

/*
 * Creates a new UUID string.
 */
unsigned char* Guid::getGuid()
{
    unsigned char* buffer = new unsigned char[16] ;

#if defined(WIN32) || defined(__CYGWIN__)
    GUID guid = GUID_NULL ;

	// Create GUID
    CoCreateGuid(&guid) ;
    if( guid == GUID_NULL )
    {
        // TODO: exception
        //cerr << "Failed to create an UUID" << endl ;
        return NULL ;
    }
	// Store GUID in internal buffer
    memcpy(buffer, &guid, 16) ;

#else
    uuid_t uuid ;

	// Create UUID
    uuid_generate(uuid) ;

	// Store UUID in internal buffer
	memcpy(buffer, uuid, 16) ;
#endif

    return buffer ;
}

/*
 *
 */
p<string> Guid::getGuidString()
{
    unsigned char* buffer = NULL ;
    char*          result = NULL ;
    p<string>      guidStr ;

    buffer = getGuid() ;
    result = new char[40] ;

    // Format into a string
    sprintf(result, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
           buffer[0],  buffer[1],  buffer[2],  buffer[3],
           buffer[4],  buffer[5],  buffer[6],  buffer[7],
           buffer[8],  buffer[9],  buffer[10], buffer[11],
           buffer[12], buffer[13], buffer[14], buffer[15]) ;

    guidStr = new string(result) ;

    // Clean up
    delete result ;
    delete buffer ;

    return guidStr ;
}
