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
 
#include "ByteArrayOutputStream.h"
#include <algorithm>

using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
void ByteArrayOutputStream::close() throw(cms::CMSException)
{  
   // Clear the Buffer
   flush();
}
      
////////////////////////////////////////////////////////////////////////////////
void ByteArrayOutputStream::flush() throw (IOException)
{
    // No Op
}

////////////////////////////////////////////////////////////////////////////////
void ByteArrayOutputStream::clear() throw (IOException)
{
   // Empty the contents of the buffer to the output stream.
   buffer.clear();
}

////////////////////////////////////////////////////////////////////////////////
void ByteArrayOutputStream::write( const unsigned char c ) 
   throw (IOException)
{
   buffer.push_back( c );  
}

////////////////////////////////////////////////////////////////////////////////    
void ByteArrayOutputStream::write( const unsigned char* buffer, 
                                   const int len ) 
   throw (IOException)
{     
   // Iterate until all the data is written.
   for( int ix = 0; ix < len; ++ix)
   {
      this->buffer.push_back( buffer[ix] );
   }  
}

