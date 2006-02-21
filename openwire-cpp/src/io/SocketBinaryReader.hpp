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
#ifndef SocketBinaryReader_hpp_
#define SocketBinaryReader_hpp_

#include <apr_network_io.h>
#include "io/BinaryReader.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace io
      {
        using namespace ifr ;

/*
 * SocketBinaryReader reads primitive C++ data types from a
 * socket stream. It currently uses APR sockets for
 * platform independency.
 */
class SocketBinaryReader : public BinaryReader
{
private:
    apr_socket_t* socket ;

public:
    SocketBinaryReader(apr_socket_t* socket) ;
    ~SocketBinaryReader() ;

    virtual void close() throw(IOException) ;
    virtual int read(char* buffer, int size) throw(IOException) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*SocketBinaryReader_hpp_*/
