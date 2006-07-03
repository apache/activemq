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
 
#include "ByteArrayInputStream.h"
#include <algorithm>

using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ByteArrayInputStream::ByteArrayInputStream()
{
   pos = buffer.end();
}

////////////////////////////////////////////////////////////////////////////////
ByteArrayInputStream::ByteArrayInputStream( const unsigned char* buffer,
                                            int bufferSize )
{
   setByteArray( buffer, bufferSize );
}

////////////////////////////////////////////////////////////////////////////////
ByteArrayInputStream::~ByteArrayInputStream(void)
{
}

////////////////////////////////////////////////////////////////////////////////
void ByteArrayInputStream::setByteArray( const unsigned char* buffer,
                                         int bufferSize )
{
   // Remove old data
   this->buffer.clear();
   
   // Copy data to internal buffer.
   for( int ix = 0; ix < bufferSize; ++ix )
   {
      this->buffer.push_back(buffer[ix]);
   }
   
   // Begin at the Beginning.
   pos = this->buffer.begin();
}

////////////////////////////////////////////////////////////////////////////////
void ByteArrayInputStream::close() throw(cms::CMSException){
	
	// Close the delegate stream.
	buffer.clear();
}

////////////////////////////////////////////////////////////////////////////////
unsigned char ByteArrayInputStream::read() throw (IOException)
{
   if(pos != buffer.end())
   {
      return *(pos++);
   }
   
   throw IOException( __FILE__, __LINE__, 
      "ByteArrayInputStream::read: Out of Data");
}

////////////////////////////////////////////////////////////////////////////////
int ByteArrayInputStream::read( unsigned char* buffer, 
	                             const int bufferSize ) 
                                   throw (IOException)
{
   int ix = 0;
   
   for( ; ix < bufferSize; ++ix, ++pos)
   {
      if(pos == this->buffer.end())
      {        
         break;
      }
      
      buffer[ix] = *(pos);
   }
   
   return ix;
}
