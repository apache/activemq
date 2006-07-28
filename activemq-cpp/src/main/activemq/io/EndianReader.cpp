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
#include "EndianReader.h"
#include "../util/Endian.h"

using namespace activemq::io;
using namespace activemq::util;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
EndianReader::EndianReader()
{
    inputStream = NULL;
}

////////////////////////////////////////////////////////////////////////////////
EndianReader::EndianReader( InputStream* is )
{
    inputStream = is;
}

////////////////////////////////////////////////////////////////////////////////
EndianReader::~EndianReader()
{}

////////////////////////////////////////////////////////////////////////////////
unsigned char EndianReader::readByte() throw( IOException )
{
    unsigned char value ;

    // Read a single byte
    read(&value, sizeof(unsigned char));
    return value;
}

////////////////////////////////////////////////////////////////////////////////
double EndianReader::readDouble() throw( IOException )
{
    double value;

    // Read a double and convert from big endian to little endian if necessary
    read( (unsigned char*)&value, sizeof( double ) );
    return Endian::byteSwap(value) ;
}

////////////////////////////////////////////////////////////////////////////////
float EndianReader::readFloat() throw( IOException )
{
    float value ;

    // Read a float and convert from big endian to little endian if necessary
    read( (unsigned char*)&value, sizeof( float ) );
    return Endian::byteSwap(value) ;
}

////////////////////////////////////////////////////////////////////////////////
uint16_t EndianReader::readUInt16() throw(IOException)
{
    uint16_t value;

    // Read a short and byteswap
    read((unsigned char*)&value, sizeof(value) );
    return Endian::byteSwap(value);
}

////////////////////////////////////////////////////////////////////////////////
uint32_t EndianReader::readUInt32() throw(IOException)
{
    uint32_t value;

    // Read an int and convert from big endian to little endian if necessary
    read( (unsigned char*)&value, sizeof( value ) );
    return Endian::byteSwap(value);
}

////////////////////////////////////////////////////////////////////////////////
uint64_t EndianReader::readUInt64() throw(IOException)
{
    uint64_t value;

    // Read a long long and convert from big endian to little endian if necessary
    read( (unsigned char*)&value, sizeof( value ) );
    return Endian::byteSwap(value);
}

////////////////////////////////////////////////////////////////////////////////
/*string EndianReader::readString() throw(IOException)
{
    // Read length of string
    short length = readShort() ;
    char* buffer = new char[length+1] ;

    // Read string bytes
    read((unsigned char*)buffer, length) ;
    *(buffer+length) = '\0' ;

    // Create string class
    string value;
    value.assign(buffer) ;

    return value ;
}*/

////////////////////////////////////////////////////////////////////////////////
int EndianReader::read( unsigned char* buffer, int count ) throw( IOException ){
	
	if( inputStream == NULL ){
		throw IOException( __FILE__, __LINE__, 
            "EndianReader::read(char*,int) - input stream is NULL" );
	}
	
	return inputStream->read( buffer, count );
}
