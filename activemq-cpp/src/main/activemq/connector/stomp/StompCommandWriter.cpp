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
#include "StompCommandWriter.h"

#include <activemq/connector/stomp/StompFrame.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>

using namespace std;
using namespace activemq;
using namespace activemq::connector;
using namespace activemq::connector::stomp;
using namespace activemq::connector::stomp::commands;
using namespace activemq::transport;
using namespace activemq::io;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
StompCommandWriter::StompCommandWriter(void)
{
    outputStream = NULL;
}

////////////////////////////////////////////////////////////////////////////////
StompCommandWriter::StompCommandWriter(OutputStream* os)
{
    outputStream = os;
}

////////////////////////////////////////////////////////////////////////////////
void StompCommandWriter::writeCommand( const Command* command ) 
    throw ( transport::CommandIOException )
{
    try
    {
        if( outputStream == NULL )
        {
            throw CommandIOException( 
                __FILE__, __LINE__, 
                "StompCommandWriter::writeCommand - "
                "output stream is NULL" );
        }

        const StompFrame& frame = marshaler.marshal( command );

        // Write the command.
        const string& cmdString = frame.getCommand();
        write( cmdString.c_str(), cmdString.length() );
        writeByte( '\n' );

        // Write all the headers.
        vector< pair<string,string> > headers = frame.getProperties().toArray();   
        for( unsigned int ix=0; ix < headers.size(); ++ix )
        {
            string& name = headers[ix].first;
            string& value = headers[ix].second;

            write( name.c_str(), name.length() );
            writeByte( ':' );
            write( value.c_str(), value.length() );
            writeByte( '\n' );       
        }

        // Finish the header section with a form feed.
        writeByte( '\n' );

        // Write the body.
        const char* body = frame.getBody();
        if( body != NULL ) 
        {
            write( body, frame.getBodyLength() );
        }

        if( ( frame.getBodyLength() == 0 ) ||
            ( frame.getProperties().getProperty( 
                  CommandConstants::toString( 
                      CommandConstants::HEADER_CONTENTLENGTH ), "" ) != "" ) )
        {
            writeByte( '\0' );
        }

        writeByte( '\n' );

        // Flush the stream.
        outputStream->flush();
    }
    AMQ_CATCH_RETHROW( CommandIOException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, CommandIOException )
    AMQ_CATCHALL_THROW( CommandIOException )
}

////////////////////////////////////////////////////////////////////////////////
void StompCommandWriter::write(const unsigned char* buffer, int count) 
    throw(IOException)
{
    if( outputStream == NULL )
    {
        throw IOException( 
            __FILE__, __LINE__, 
            "StompCommandWriter::write(char*,int) - input stream is NULL" );
    }

    outputStream->write( buffer, count );
}
 
////////////////////////////////////////////////////////////////////////////////
void StompCommandWriter::writeByte(unsigned char v) throw(IOException)
{
    if( outputStream == NULL )
    {
        throw IOException( 
            __FILE__, __LINE__, 
            "StompCommandWriter::write(char) - input stream is NULL" );
    }
   
    outputStream->write( v );
}

////////////////////////////////////////////////////////////////////////////////
void StompCommandWriter::write(const char* buffer, int count) 
   throw(io::IOException)
{
    write(reinterpret_cast<const unsigned char*>(buffer), count);
}
