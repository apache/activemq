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
 
#include "BufferedSocket.h"

#include <activemq/exceptions/IllegalArgumentException.h>

using namespace activemq;
using namespace activemq::network;
using namespace activemq::io;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
BufferedSocket::BufferedSocket(Socket* socket,
                               unsigned int inputBufferSize,
                               unsigned int outputBufferSize,
                               bool own)
{
   if(socket == NULL)
   {
      throw IllegalArgumentException(
         __FILE__, __LINE__,
         "BufferedSocket::BufferedSocket - Constructed with NULL Socket");
   }
   
   this->socket = socket;
   this->inputBufferSize = inputBufferSize;
   this->outputBufferSize = outputBufferSize;
   this->own = own;
}

////////////////////////////////////////////////////////////////////////////////
BufferedSocket::~BufferedSocket(void)
{
   try
   {
      if(outputStream)
      {
         // Ensure all data is written
         outputStream->flush();
      }

      // Close the socket      
      socket->close();
         
      // if we own it, delete it.
      if(own)
      {
         delete socket;
      }
      
      // Clean up our streams.
      delete inputStream;
      delete outputStream;
   }
   AMQ_CATCH_NOTHROW( ActiveMQException )
   AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void BufferedSocket::connect( const char* host, const int port ) 
   throw( SocketException )
{
   try
   {      
      if( socket->isConnected() )
      {
         throw SocketException( __FILE__, __LINE__, 
               "BufferedSocket::connect() - socket already connected" );
      }

      // Connect the socket.
      socket->connect( host, port );

      // Now create the buffered streams that wrap around the socket.
      inputStream = new BufferedInputStream( 
         socket->getInputStream(), inputBufferSize );
      outputStream = new BufferedOutputStream( 
         socket->getOutputStream(), outputBufferSize );
   }
   AMQ_CATCH_RETHROW( SocketException )
   AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, SocketException )
   AMQ_CATCHALL_THROW( SocketException )   
}

////////////////////////////////////////////////////////////////////////////////
void BufferedSocket::close(void) throw( cms::CMSException )
{
   try
   {
      // Ensure all data writen
      outputStream->flush();
      
      // Close the Socket
      socket->close();
      
      // Remove old stream, recreate if reconnected
      delete inputStream;
      delete outputStream;
      
      inputStream = NULL;
      outputStream = NULL;
   }
   AMQ_CATCH_RETHROW( SocketException )
   AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, SocketException )
   AMQ_CATCHALL_THROW( SocketException )   
}
