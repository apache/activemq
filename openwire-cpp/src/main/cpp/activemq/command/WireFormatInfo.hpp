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
#ifndef ActiveMQ_WireFormatInfo_hpp_
#define ActiveMQ_WireFormatInfo_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include <map>
#include "activemq/command/AbstractCommand.hpp"
#include "activemq/protocol/IMarshaller.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
      using namespace ifr;
      using namespace std;
      using namespace apache::activemq;
      using namespace apache::activemq::protocol;
      using namespace apache::ppr::io;
      using namespace apache::ppr::util;

/*
 *  Marshalling code for Open Wire Format for WireFormatInfo
 */
class WireFormatInfo : public AbstractCommand
{
protected:
    array<char> magic ;
    int         version,
                propsByteSize ;
    p<PropertyMap> properties ;

public:
    const static unsigned char TYPE = 1;

public:
    WireFormatInfo() ;

    virtual unsigned char getDataStructureType() ;

    virtual array<char> getMagic() ;
    virtual void setMagic(array<char> magic) ;

    virtual int getVersion() ;
    virtual void setVersion(int version) ;

    virtual bool getCacheEnabled() ;
    virtual void setCacheEnabled(bool cacheEnabled) ;

    virtual bool getStackTraceEnabled() ;
    virtual void setStackTraceEnabled(bool stackTraceEnabled) ;

    virtual bool getTcpNoDelayEnabled() ;
    virtual void setTcpNoDelayEnabled(bool tcpNoDelayEnabled) ;

    virtual bool getSizePrefixDisabled() ;
    virtual void setSizePrefixDisabled(bool sizePrefixDisabled) ;

    virtual bool getTightEncodingEnabled() ;
    virtual void setTightEncodingEnabled(bool tightEncodingEnabled) ;

    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException) ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_WireFormatInfo_hpp_*/
