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
#ifndef ACTIVEMQ_NETWORK_SOCKET_H
#define ACTIVEMQ_NETWORK_SOCKET_H

#include <activemq/network/SocketException.h>
#include <activemq/network/Socket.h>
#include <activemq/io/InputStream.h>
#include <activemq/io/OutputStream.h>

namespace activemq{
namespace network{
   
    // Forward declarations
    class SocketInputStream;
    class SocketOutputStream;
   
    /**
     * Platform-independent implementation of the socket interface.
     */
    class TcpSocket : public Socket
    {      
    private:
   
        /**
         * The handle for this socket.
         */
         SocketHandle socketHandle;
       
         /**
          * The input stream for reading this socket.
          */
         SocketInputStream* inputStream;
       
         /**
          * The output stream for writing to this socket.
          */
         SocketOutputStream* outputStream;
       
    public:
   
        /** 
         * Construct a non-connected socket.
         */
        TcpSocket();
 
        /** 
         * Construct a connected or bound socket based on given
         * socket handle.
         * @param socketHandle a socket handle to wrap in the object
         */
        TcpSocket( SocketHandle socketHandle );
       
        /**
         * Destruct.
         * Releases the socket handle but not
         * gracefully shut down the connection.
         */
        virtual ~TcpSocket();
       
        /**
         * Gets the handle for the socket.
         * @return SocketHabler for this Socket, can be NULL
         */
        SocketHandle getSocketHandle () {
            return socketHandle;
        }
   
        /**
         * Connects to the specified destination. Closes this socket if 
         * connected to another destination.
         * @param host The host of the server to connect to.
         * @param port The port of the server to connect to.
         * @throws IOException Thrown if a failure occurred in the connect.
         */
        virtual void connect( const char* host, const int port ) throw( SocketException );
      
        /**
         * Indicates whether or not this socket is connected to a destination.
         * @return true if connected
         */
        virtual bool isConnected() const{
            return socketHandle != INVALID_SOCKET_HANDLE;
        }
      
        /**
         * Gets the InputStream for this socket.
         * @return The InputStream for this socket. NULL if not connected.
         */
        virtual io::InputStream* getInputStream();
      
        /**
         * Gets the OutputStream for this socket.
         * @return the OutputStream for this socket.  NULL if not connected.
         */
        virtual io::OutputStream* getOutputStream();
      
        /**
         * Gets the linger time.
         * @return The linger time in seconds.
         * @throws SocketException if the operation fails.
         */
        virtual int getSoLinger() const throw( SocketException );
      
        /**
         * Sets the linger time.
         * @param linger The linger time in seconds.  If 0, linger is off.
         * @throws SocketException if the operation fails.
         */
        virtual void setSoLinger( const int linger ) throw( SocketException );
      
        /**
         * Gets the keep alive flag.
         * @return True if keep alive is enabled.
         * @throws SocketException if the operation fails.
         */
        virtual bool getKeepAlive() const throw( SocketException );
      
        /**
         * Enables/disables the keep alive flag.
         * @param keepAlive If true, enables the flag.
         * @throws SocketException if the operation fails.
         */
        virtual void setKeepAlive( const bool keepAlive ) throw( SocketException );
      
        /**
         * Gets the receive buffer size.
         * @return the receive buffer size in bytes.
         * @throws SocketException if the operation fails.
         */
        virtual int getReceiveBufferSize() const throw( SocketException );
      
        /**
         * Sets the recieve buffer size.
         * @param size Number of bytes to set the receive buffer to.
         * @throws SocketException if the operation fails.
         */
        virtual void setReceiveBufferSize( const int size ) throw( SocketException );
      
        /**
         * Gets the reuse address flag.
         * @return True if the address can be reused.
         * @throws SocketException if the operation fails.
         */
        virtual bool getReuseAddress() const throw( SocketException );
      
        /**
         * Sets the reuse address flag.
         * @param reuse If true, sets the flag.
         * @throws SocketException if the operation fails.
         */
        virtual void setReuseAddress( const bool reuse ) throw( SocketException );
      
        /**
         * Gets the send buffer size.
         * @return the size in bytes of the send buffer.
         * @throws SocketException if the operation fails.
         */
        virtual int getSendBufferSize() const throw( SocketException );
      
        /**
         * Sets the send buffer size.
         * @param size The number of bytes to set the send buffer to.
         * @throws SocketException if the operation fails.
         */
        virtual void setSendBufferSize( const int size ) throw( SocketException );
      
        /**
         * Gets the timeout for socket operations.
         * @return The timeout in milliseconds for socket operations.
         * @throws SocketException Thrown if unable to retrieve the information.
         */
        virtual int getSoTimeout() const throw( SocketException );
      
        /**
         * Sets the timeout for socket operations.
         * @param timeout The timeout in milliseconds for socket operations.<p>
         * @throws SocketException Thrown if unable to set the information.
         */
        virtual void setSoTimeout( const int timeout ) throw(SocketException);

        /**
         * Closes this object and deallocates the appropriate resources.
         * @throws CMSException
         */
        virtual void close() throw( cms::CMSException );
       
    protected:
   
        #if !defined( unix ) || defined( __CYGWIN__ )
      
            // WINDOWS needs initialization of winsock
            class StaticSocketInitializer {
            private:
          
                SocketException* socketInitError;
              
                void clear(){
                    if( socketInitError != NULL ){
                        delete socketInitError;
                    }
                    socketInitError = NULL;
                }
              
            public:

                SocketException* getSocketInitError() {
                    return socketInitError;
                }

                StaticSocketInitializer();
                virtual ~StaticSocketInitializer();

            };
          
            static StaticSocketInitializer staticSocketInitializer;
        #endif
   
    };

}}

#endif /*ACTIVEMQ_NETWORK_SOCKET_H*/
