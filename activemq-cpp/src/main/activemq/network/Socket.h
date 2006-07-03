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
#ifndef _ACTIVEMQ_NETWORK_SOCKET_H_
#define _ACTIVEMQ_NETWORK_SOCKET_H_

#include <activemq/network/SocketException.h>
#include <activemq/io/InputStream.h>
#include <activemq/io/OutputStream.h>
#include <cms/Closeable.h>

#if !(defined( unix ) || defined(__APPLE__)) || defined( __CYGWIN__ )
#include <Winsock2.h> // SOCKET
#endif

namespace activemq{
namespace network{

   class Socket : public cms::Closeable
   {
   public:
   
      // Define the SocketHandle type.
      #if defined( unix ) || defined(__APPLE__) && !defined( __CYGWIN__ )
          typedef int SocketHandle;
      #else
          typedef SOCKET SocketHandle;
      #endif

      /**
       * Defines a constant for an invalid socket handle.
       */   
       static const SocketHandle INVALID_SOCKET_HANDLE = (SocketHandle) -1;

   public:

      /**
       * Destructor
       */
   	virtual ~Socket(void) {}

       /**
       * Connects to the specified destination. Closes this socket if 
       * connected to another destination.
       * @param host The host of the server to connect to.
       * @param port The port of the server to connect to.
       * @throws IOException Thrown if a failure occurred in the connect.
       */
      virtual void connect( const char* host, const int port ) 
         throw(SocketException) = 0;
      
      /**
       * Indicates whether or not this socket is connected to a destination.
       */
      virtual bool isConnected() const = 0;      

      /**
       * Gets the InputStream for this socket.
       * @return The InputStream for this socket. NULL if not connected.
       */
      virtual io::InputStream* getInputStream() = 0;
      
      /**
       * Gets the OutputStream for this socket.
       * @return the OutputStream for this socket.  NULL if not connected.
       */
      virtual io::OutputStream* getOutputStream() = 0;

      /**
       * Gets the linger time.
       * @return The linger time in seconds.
       * @throws SocketException if the operation fails.
       */
      virtual int getSoLinger() const throw(SocketException) = 0;
      
      /**
       * Sets the linger time.
       * @param linger The linger time in seconds.  If 0, linger is off.
       * @throws SocketException if the operation fails.
       */
      virtual void setSoLinger( const int linger ) throw(SocketException) = 0;
      
      /**
       * Gets the keep alive flag.
       * @return True if keep alive is enabled.
       * @throws SocketException if the operation fails.
       */
      virtual bool getKeepAlive() const throw(SocketException) = 0;
      
      /**
       * Enables/disables the keep alive flag.
       * @param keepAlive If true, enables the flag.
       * @throws SocketException if the operation fails.
       */
      virtual void setKeepAlive( const bool keepAlive ) throw(SocketException) = 0;
      
      /**
       * Gets the receive buffer size.
       * @return the receive buffer size in bytes.
       * @throws SocketException if the operation fails.
       */
      virtual int getReceiveBufferSize() const throw(SocketException) = 0;
      
      /**
       * Sets the recieve buffer size.
       * @param size Number of bytes to set the receive buffer to.
       * @throws SocketException if the operation fails.
       */
      virtual void setReceiveBufferSize( const int size ) throw(SocketException) = 0;
      
      /**
       * Gets the reuse address flag.
       * @return True if the address can be reused.
       * @throws SocketException if the operation fails.
       */
      virtual bool getReuseAddress() const throw(SocketException) = 0;
      
      /**
       * Sets the reuse address flag.
       * @param reuse If true, sets the flag.
       * @throws SocketException if the operation fails.
       */
      virtual void setReuseAddress( const bool reuse ) throw(SocketException) = 0;
      
      /**
       * Gets the send buffer size.
       * @return the size in bytes of the send buffer.
       * @throws SocketException if the operation fails.
       */
      virtual int getSendBufferSize() const throw(SocketException) = 0;
      
      /**
       * Sets the send buffer size.
       * @param size The number of bytes to set the send buffer to.
       * @throws SocketException if the operation fails.
       */
      virtual void setSendBufferSize( const int size ) throw(SocketException) = 0;
      
      /**
       * Gets the timeout for socket operations.
       * @return The timeout in milliseconds for socket operations.
       * @throws SocketException Thrown if unable to retrieve the information.
       */
      virtual int getSoTimeout() const throw(SocketException) = 0;
      
      /**
       * Sets the timeout for socket operations.
       * @param timeout The timeout in milliseconds for socket operations.<p>
       * @throws SocketException Thrown if unable to set the information.
       */
      virtual void setSoTimeout( const int timeout ) throw(SocketException) = 0;

   };

}}

#endif /*_ACTIVEMQ_NETWORK_BASESOCKET_H_*/
