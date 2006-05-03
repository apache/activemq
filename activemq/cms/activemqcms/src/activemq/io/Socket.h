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

#ifndef ACTIVEMQ_IO_SOCKET_H_
#define ACTIVEMQ_IO_SOCKET_H_
 
#include <cms/Closeable.h>
#include <activemq/io/IOException.h>
#include <activemq/io/SocketException.h>
#include <activemq/io/InputStream.h>
#include <activemq/io/OutputStream.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <string>
#include <arpa/inet.h>

namespace activemq{
namespace io{
	
	/**
	 * A basic wrapper around a socket.  The interface
	 * attempts to match (as much as makes sense) the Java Socket API.
	 * @author Nathan Mittler
	 */
	class Socket : public cms::Closeable
	{
	public:
	
		/**
		 * Default constructor - does nothing.  Connect must be called to
		 * connect to a destination.
		 */
		Socket();
		
		/**
		 * Constructor - connects to a destination.
		 * @param host The host of the server to connect to.
		 * @param port The port of the server to connect to.
		 * @throws IOException Thrown if a failure occurred in the connect.
		 */
		Socket( const char* host, const int port ) throw(IOException);	
		
		/**
		 * Closes the socket if necessary.
		 */	
		virtual ~Socket();
		
        virtual int getHandle() const{ return m_socket; }
        
		/**
		 * Connects to the specified destination. Closes this socket if 
		 * connected to another destination.
		 * @param host The host of the server to connect to.
		 * @param port The port of the server to connect to.
		 * @throws IOException Thrown if a failure occurred in the connect.
		 */
		virtual void connect( const char* host, const int port ) throw(IOException);
		
		/**
		 * Indicates whether or not this socket is connected to a destination.
		 */
		virtual bool isConnected() const{ return m_socket > 0; }
		
		/**
		 * Gets the InputStream for this socket.
		 * @return The InputStream for this socket. NULL if not connected.
		 */
		virtual InputStream* getInputStream(){
			return inputStream;
		}
		
		/**
		 * Gets the OutputStream for this socket.
		 * @return the OutputStream for this socket.  NULL if not connected.
		 */
		virtual OutputStream* getOutputStream(){
			return outputStream;
		}
		
		/**
		 * Gets the local host.
		 */
		//virtual const char* getLocalHost() const;
		
		/**
		 * Gets the local port.
		 */
		//virtual int getLocalPort() const;
		
		/**
		 * Gets the remote host.
		 */
		//virtual const char* getRemoteHost() const;
		
		/**
		 * Gets the remote port.
		 */
		//virtual int getRemotePort() const;
		
		/**
		 * Closes the current connection, if necessary.
		 * @throws IOException if an error occurs in shutdown.
		 */
		virtual void close() throw( cms::CMSException );		
		
		////////////////// SOCKET OPTIONS ////////////////////////
		
		/**
		 * Gets the linger time.
		 * @return The linger time in microseconds.
		 * @throws SocketException if the operation fails.
		 */
		virtual int getSoLinger() const throw(SocketException);
		
		/**
		 * Sets the linger time.
		 * @param linger The linger time in microseconds.  If 0, linger is off.
		 * @throws SocketException if the operation fails.
		 */
		virtual void setSoLinger( const int linger ) throw(SocketException);
		
		/**
		 * Gets the keep alive flag.
		 * @return True if keep alive is enabled.
		 * @throws SocketException if the operation fails.
		 */
		virtual bool getKeepAlive() const throw(SocketException);
		
		/**
		 * Enables/disables the keep alive flag.
		 * @param keepAlive If true, enables the flag.
		 * @throws SocketException if the operation fails.
		 */
		virtual void setKeepAlive( const bool keepAlive ) throw(SocketException);
		
		/**
		 * Gets the TcpNoDelay flag.
		 * @return The TcpNoDelay flag.
		 * @throws SocketException if the operation fails.
		 */
		//virtual bool getTcpNoDelay() const throw(SocketException);
		
		/**
		 * Enables/disables the TcpNoDelay flag.
		 * @param noDelay If true, enables the flag.
		 * @throws SocketException if the operation fails.
		 */
		//virtual void setTcpNoDelay( const bool noDelay ) throw(SocketException);
		
		/**
		 * Gets the receive buffer size.
		 * @return the receive buffer size in bytes.
		 * @throws SocketException if the operation fails.
		 */
		virtual int getReceiveBufferSize() const throw(SocketException);
		
		/**
		 * Sets the recieve buffer size.
		 * @param size Number of bytes to set the receive buffer to.
		 * @throws SocketException if the operation fails.
		 */
		virtual void setReceiveBufferSize( const int size ) throw(SocketException);
		
		/**
		 * Gets the reuse address flag.
		 * @return True if the address can be reused.
		 * @throws SocketException if the operation fails.
		 */
		virtual bool getReuseAddress() const throw(SocketException);
		
		/**
		 * Sets the reuse address flag.
		 * @param reuse If true, sets the flag.
		 * @throws SocketException if the operation fails.
		 */
		virtual void setReuseAddress( const bool reuse ) throw(SocketException);
		
		/**
		 * Gets the send buffer size.
		 * @return the size in bytes of the send buffer.
		 * @throws SocketException if the operation fails.
		 */
		virtual int getSendBufferSize() const throw(SocketException);
		
		/**
		 * Sets the send buffer size.
		 * @param size The number of bytes to set the send buffer to.
		 * @throws SocketException if the operation fails.
		 */
		virtual void setSendBufferSize( const int size ) throw(SocketException);
		
		/**
		 * Gets the timeout for socket operations.
		 * @return The timeout in microseconds for socket operations.
		 * @throws SocketException Thrown if unable to retrieve the information.
		 */
		virtual int getSoReceiveTimeout() const throw(SocketException);
		
		/**
		 * Sets the timeout for socket operations.
		 * @param timeout The timeout in microseconds for socket operations.<p>
		 * @throws SocketException Thrown if unable to set the information.
		 */
		virtual void setSoReceiveTimeout( const int timeout ) throw(SocketException);
		
		/**
		 * Gets the timeout for socket operations.
		 * @return The timeout in microseconds for socket operations.
		 * @throws SocketException Thrown if unable to retrieve the information.
		 */
		virtual int getSoSendTimeout() const throw(SocketException);
		
		/**
		 * Sets the timeout for socket send operations.
		 * @param timeout The timeout in microseconds for socket operations.<p>
		 * @throws SocketException Thrown if unable to set the information.
		 */
		virtual void setSoSendTimeout( const int timeout ) throw(SocketException);
		
	private:
	
		/**
		 * Initializes all members.
		 */
		void init();
		
	private:
	
		/**
		 * The socket handle.
		 */
		int m_socket;
		
		/**
		 * The socket address.
		 */
  		sockaddr_in addressIn;  
		
		/**
		 * The input stream for this socket.
		 */
		InputStream* inputStream;
		
		/**
		 * The output stream for this socket.
		 */
		OutputStream* outputStream;		
	};

}}

#endif /*ACTIVEMQ_IO_SOCKET_H_*/
