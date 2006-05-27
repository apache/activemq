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
#ifndef ActiveMQ_OpenWireFormat_hpp_
#define ActiveMQ_OpenWireFormat_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include "activemq/IDataStructure.hpp"
#include "activemq/command/WireFormatInfo.hpp"
#include "activemq/protocol/IProtocol.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/io/DataInputStream.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace protocol
    {
      namespace openwire
      {
          class OpenWireMarshaller ;
      }
      using namespace ifr;
      using namespace apache::activemq;
      using namespace apache::activemq::command;
      using namespace apache::activemq::protocol::openwire;
      using namespace apache::ppr::io;

/*
 * Represents the wire format.
 */
class OpenWireProtocol : public IProtocol
{
private:
    p<OpenWireMarshaller> wireMarshaller ;
    p<WireFormatInfo>     wireFormatInfo ;
    bool                  sizePrefixDisabled ;

    static const char NULL_TYPE ;
    static const int  PROTOCOL_VERSION ;

    static const char MAGIC[8] ;

public:
    OpenWireProtocol() ;

	virtual p<WireFormatInfo> getWireFormatInfo() ;
	virtual bool getStackTraceEnabled() ;

    virtual void handshake(p<ITransport> transport) ;
	virtual void marshal(p<IDataStructure> object, p<IOutputStream> ostream) throw(IOException) ;
	virtual p<IDataStructure> unmarshal(p<IInputStream> istream) throw(IOException) ;

protected:
    p<DataOutputStream> checkOutputStream(p<IOutputStream> ostream) throw (IOException) ;
    p<DataInputStream> checkInputStream(p<IInputStream> istream) throw (IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_OpenWireFormat_hpp_*/
