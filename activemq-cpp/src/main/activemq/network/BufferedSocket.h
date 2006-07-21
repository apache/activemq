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
#ifndef _ACTIVEMQ_NETWORK_BUFFEREDSOCKET_H_
#define _ACTIVEMQ_NETWORK_BUFFEREDSOCKET_H_

#include <activemq/network/Socket.h>
#include <activemq/network/SocketException.h>
#include <activemq/io/BufferedInputStream.h>
#include <activemq/io/BufferedOutputStream.h>

namespace activemq{
namespace network{

    /**
     * Buffered Socket class that wraps a <code>Socket</code> derived
     * object and provides Buffered input and Output Streams to improce
     * the efficiency of the reads and writes.
     */
    class BufferedSocket : public Socket
    {
    private:
   
        // Socket that this class wraps to provide buffering
        Socket* socket;
      
        // Indicates if the lifetime of the Socket is controlled by this 
        // class.  If true Socket is deleted at destruction.
        bool own;
      
        // Buffered Input stream to wrap the Socket input stream
        io::BufferedInputStream* inputStream;
      
        // Buffered Output stream to wrap the Socket input stream
        io::BufferedOutputStream* outputStream;
      
        // Sizes for the Buffered Streams
        unsigned int inputBufferSize;
        unsigned int outputBufferSize;

    public:

        BufferedSocket( Socket* socket, 
                        unsigned int inputBufferSize = 1000,
                        unsigned int outputBufferSize = 1000,
                        bool own = true );

        virtual ~BufferedSocket(void);

        /**
         * Connects to the specified destination. Closes this socket if 
         * connected to another destination.
         * @param host The host of the server to connect to.
         * @param port The port of the server to connect to.
         * @throws IOException Thrown if a failure occurred in the connect.
         */
        virtual void connect( const char* host, const int port ) 
            throw( SocketException );
      
        /**
         * Closes this object and deallocates the appropriate resources.
         * @throws CMSException
         */
        virtual void close() throw( cms::CMSException );

        /**
         * Indicates whether or not this socket is connected to a destination.
         * @return true if connected
         */
        virtual bool isConnected() const{
            return socket->isConnected();
        }

        /**
         * Gets the InputStream for this socket.
         * @return The InputStream for this socket. NULL if not connected.
         */
        virtual io::InputStream* getInputStream(){
            return inputStream;
        }
      
        /**
         * Gets the OutputStream for this socket.
         * @return the OutputStream for this socket.  NULL if not connected.
         */
        virtual io::OutputStream* getOutputStream(){
            return outputStream;
        }

        /**
         * Gets the linger time.
         * @return The linger time in seconds.
         * @throws SocketException if the operation fails.
         */
        virtual int getSoLinger() const throw( SocketException ){
            return socket->getSoLinger();
        }
      
        /**
         * Sets the linger time.
         * @param linger The linger time in seconds.  If 0, linger is off.
         * @throws SocketException if the operation fails.
         */
        virtual void setSoLinger( const int linger ) throw( SocketException ){
            socket->setSoLinger( linger );
        }
      
        /**
         * Gets the keep alive flag.
         * @return True if keep alive is enabled.
         * @throws SocketException if the operation fails.
         */
        virtual bool getKeepAlive() const throw( SocketException ){
            return socket->getKeepAlive();
        }
      
        /**
         * Enables/disables the keep alive flag.
         * @param keepAlive If true, enables the flag.
         * @throws SocketException if the operation fails.
         */
        virtual void setKeepAlive( const bool keepAlive ) throw( SocketException ){
            socket->setKeepAlive( keepAlive );
        }
      
        /**
         * Gets the receive buffer size.
         * @return the receive buffer size in bytes.
         * @throws SocketException if the operation fails.
         */
        virtual int getReceiveBufferSize() const throw( SocketException ){
            return socket->getReceiveBufferSize();
        }
      
        /**
         * Sets the recieve buffer size.
         * @param size Number of bytes to set the receive buffer to.
         * @throws SocketException if the operation fails.
         */
        virtual void setReceiveBufferSize( const int size ) throw( SocketException ){
            socket->setReceiveBufferSize( size );
        }
      
        /**
         * Gets the reuse address flag.
         * @return True if the address can be reused.
         * @throws SocketException if the operation fails.
         */
        virtual bool getReuseAddress() const throw( SocketException ){
            return socket->getReuseAddress();
        }
      
        /**
         * Sets the reuse address flag.
         * @param reuse If true, sets the flag.
         * @throws SocketException if the operation fails.
         */
        virtual void setReuseAddress( const bool reuse ) throw( SocketException ){
            socket->setReuseAddress( reuse );
        }
      
        /**
         * Gets the send buffer size.
         * @return the size in bytes of the send buffer.
         * @throws SocketException if the operation fails.
         */
        virtual int getSendBufferSize() const throw( SocketException ){
            return socket->getSendBufferSize();
        }
      
        /**
         * Sets the send buffer size.
         * @param size The number of bytes to set the send buffer to.
         * @throws SocketException if the operation fails.
         */
        virtual void setSendBufferSize( const int size ) throw( SocketException ){
            socket->setSendBufferSize( size );
        }
      
        /**
         * Gets the timeout for socket operations.
         * @return The timeout in milliseconds for socket operations.
         * @throws SocketException Thrown if unable to retrieve the information.
         */
        virtual int getSoTimeout() const throw( SocketException ){
            return socket->getSoTimeout();
        }
      
        /**
         * Sets the timeout for socket operations.
         * @param timeout The timeout in milliseconds for socket operations.<p>
         * @throws SocketException Thrown if unable to set the information.
         */
        virtual void setSoTimeout( const int timeout ) throw( SocketException ){
            socket->setSoTimeout( timeout );
        }

    };

}}

#endif /*_ACTIVEMQ_NETWORK_BUFFEREDSOCKET_H_*/
