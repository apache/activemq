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
#ifndef ACTIVEMQ_NETWORK_SERVERSOCKETIMPL_H
#define ACTIVEMQ_NETWORK_SERVERSOCKETIMPL_H

#include <activemq/network/TcpSocket.h>
#include <activemq/network/SocketException.h>

namespace activemq{
namespace network{

    /**
     * A server socket class (for testing purposes).
     */
    class ServerSocket
    {
    public:
    
        typedef Socket::SocketHandle SocketHandle;
        
    private:
    
        SocketHandle socketHandle;
        
    public:
    
        /** 
         * Constructor.
         * Creates a non-bound server socket.
         */
        ServerSocket();
    
        /**
         * Destructor.
         * Releases socket handle if close() hasn't been called.
         */
        virtual ~ServerSocket();
        
    public:
    
        /**
         * Bind and listen to given IP/dns and port.
         * @param host IP address or host name.
         * @param port TCP port between 1..655535
         */
        virtual void bind( const char* host, int port ) throw ( SocketException );
    
        /**
         * Bind and listen to given IP/dns and port.
         * @param host IP address or host name.
         * @param port TCP port between 1..655535
         * @param backlog Size of listen backlog.
         */
        virtual void bind( const char* host, int port, int backlog ) throw ( SocketException );
    
        /**
         * Blocks until a client connects to the bound socket.
         * @return new socket. Never returns NULL.
         */
        virtual Socket* accept () throw ( SocketException );
    
        /**
         * Closes the server socket.
         */
        virtual void close() throw( cms::CMSException );
    
        /**
         * @return true of the server socket is bound.
         */ 
        virtual bool isBound() const;
       
   protected:

      #if !defined( unix ) || defined( __CYGWIN__ )
      
          // WINDOWS needs initialization of winsock
          class StaticServerSocketInitializer {
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
              StaticServerSocketInitializer();
              virtual ~StaticServerSocketInitializer();
                      
          };
          static StaticServerSocketInitializer staticSocketInitializer;
      #endif
      
   };

}}

#endif // ACTIVEMQ_NETWORK_SERVERSOCKETIMPL_H

