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
 
#include "EndianWriter.h"
#include <activemq/util/Endian.h>

using namespace activemq::io;
using namespace activemq::util;

////////////////////////////////////////////////////////////////////////////////
EndianWriter::EndianWriter()
{
    outputStream = NULL;
}

////////////////////////////////////////////////////////////////////////////////
EndianWriter::EndianWriter( OutputStream* os )
{
    outputStream = os;
}

////////////////////////////////////////////////////////////////////////////////
EndianWriter::~EndianWriter()
{
    // no-op
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeByte(unsigned char value) throw(IOException)
{
    // Write a single byte
    write(&value, sizeof(unsigned char));
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeDouble(double v) throw(IOException)
{
    // Write a double, byteswap if necessary
    double value = Endian::byteSwap(v);
    write((unsigned char*)&value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeFloat(float v) throw(IOException)
{
    // Write a float, byteswap if necessary
    float value = Endian::byteSwap(v);
    write((unsigned char*)&value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeUInt16(uint16_t v) throw(IOException)
{
    // Write a short, byteswap if necessary
    uint16_t value = Endian::byteSwap(v) ;
    write((unsigned char*)&value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeUInt32(uint32_t v) throw(IOException)
{
    // Write an int, byteswap if necessary
    uint32_t value = Endian::byteSwap(v);
    write((unsigned char*)&value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::writeUInt64(uint64_t v) throw(IOException)
{
    // Write a long long, byteswap if necessary
    uint64_t value = Endian::byteSwap(v);
    write((unsigned char*)&value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////////
/*void EndianWriter::writeString(const std::string& value) throw(IOException)
{
    // Assert argument
    if( value.length() > USHRT_MAX ){
        throw IOException("String length exceeds maximum length") ;
    }
    
    // Write length of string
    short length = (short)value.length() ;
    writeShort( length ) ;

    // Write string contents
    write((unsigned char*)value.c_str(), length) ;
}*/

////////////////////////////////////////////////////////////////////////////////
void EndianWriter::write(const unsigned char* buffer, int count) throw(IOException){
	
	if( outputStream == NULL ){
		throw IOException( __FILE__, __LINE__, 
            "EndianWriter::write(char*,int) - input stream is NULL" );
	}
	
	outputStream->write( buffer, count );
}
