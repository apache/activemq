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
#ifndef ActiveMQ_IDataStructure_hpp_
#define ActiveMQ_IDataStructure_hpp_

#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace protocol
    {
      struct IMarshaller ;
    }
    using namespace ifr;
    using namespace apache::activemq::protocol;
    using namespace apache::ppr::io;

/*
 * An OpenWire data structure.
 */
struct IDataStructure : Interface
{
    virtual unsigned char getDataStructureType() = 0 ;
    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) = 0 ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) = 0 ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_IDataStructure_hpp_*/
