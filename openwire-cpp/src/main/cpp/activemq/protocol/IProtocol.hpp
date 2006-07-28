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
#ifndef ActiveMQ_IProtocol_hpp_
#define ActiveMQ_IProtocol_hpp_

#include "activemq/IDataStructure.hpp"
#include "activemq/transport/ITransport.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace protocol
    {
      using namespace ifr ;
      using namespace apache::activemq;
      using namespace apache::activemq::transport;
      using namespace apache::ppr::io;

/*
 * Represents the logical protocol layer.
 */
struct IProtocol : Interface
{
    virtual void handshake(p<ITransport> transport) = 0 ;
	virtual void marshal(p<IDataStructure> object, p<IOutputStream> ostream) throw(IOException) = 0 ;
	virtual p<IDataStructure> unmarshal(p<IInputStream> istream) throw(IOException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_IProtocol_hpp_*/
