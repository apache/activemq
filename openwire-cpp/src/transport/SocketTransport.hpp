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
#ifndef SocketTransport_hpp_
#define SocketTransport_hpp_

#include <map>
//#include <string>

#include <apr_general.h>
#include <apr_pools.h>
#include <apr_network_io.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>

#include "OpenWireException.hpp"
#include "BrokerException.hpp"
#include "FutureResponse.hpp"
#include "command/ICommand.hpp"
#include "command/BaseCommand.hpp"
#include "command/Response.hpp"
#include "command/ExceptionResponse.hpp"
#include "io/SocketBinaryReader.hpp"
#include "io/SocketBinaryWriter.hpp"
#include "marshal/CommandMarshallerRegistry.hpp"
#include "transport/ITransport.hpp"
#include "util/SimpleMutex.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace transport
      {
        using namespace ifr ;
        using namespace std;
        using namespace apache::activemq::client;
        using namespace apache::activemq::client::command;
        using namespace apache::activemq::client::io;
        using namespace apache::activemq::client::marshal;
        using namespace apache::activemq::client::util;

/*
 * An implementation of ITransport that uses sockets to communicate with
 * the broker.
 */
class SocketTransport : public ITransport
{
private:
    p<SocketBinaryReader>        reader ;
    p<SocketBinaryWriter>        writer ;
    apr_pool_t*                  memoryPool ;
    apr_thread_mutex_t*          mutex ;
    apr_socket_t*                socket ;
    apr_sockaddr_t              *local_sa, *remote_sa ;
    char                        *local_ip, *remote_ip ;
    apr_thread_t*                readThread ;
    map<int, p<FutureResponse> > requestMap ;
    bool                         closed ;
    long                         nextCommandId ;

public:
    SocketTransport(const char* host, int port) ;
    virtual ~SocketTransport() ;

    virtual void oneway(p<ICommand> command) ;
    virtual p<FutureResponse> asyncRequest(p<ICommand> command) ;
    virtual p<Response> request(p<ICommand> command) ;

protected:
    virtual void init() ;
    virtual void send(p<ICommand> command) ;
    virtual long getNextCommandId() ;
    virtual apr_socket_t* connect(const char* host, int port) ;

private:
    virtual void acquireLock() ;
    virtual void releaseLock() ;

    static void* APR_THREAD_FUNC readLoop(apr_thread_t* thread, void *data) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*SocketTransport_hpp_*/
