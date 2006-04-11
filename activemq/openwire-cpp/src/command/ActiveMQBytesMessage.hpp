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
#ifndef ActiveMQBytesMessage_hpp_
#define ActiveMQBytesMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <map>
#include <string>
#include "IBytesMessage.hpp"
#include "ActiveMQMessage.hpp"
#include "MessageEOFException.hpp"
#include "MessageNotWritableException.hpp"
#include "util/Endian.hpp"
#include "util/ifr/ap.hpp"
#include "util/ifr/p.hpp"
#include "util/MapItemHolder.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr::v1;
        using namespace apache::activemq::client::util;

/*
 * 
 */
class ActiveMQBytesMessage : public ActiveMQMessage, IBytesMessage
{
private:
    char* body ;
    int   bodySize,
          bodyLength,
          offset ;
    bool  readMode ;

    const static int INITIAL_SIZE = 512 ;
    const static int EXPAND_SIZE  = 128 ;

public:
    const static int TYPE = 24 ;

public:
    ActiveMQBytesMessage() ;
    virtual ~ActiveMQBytesMessage() ;

    virtual int getCommandType() ;
    virtual int getBodyLength() ;

    virtual void reset() ;
    virtual char readByte() throw (MessageNotReadableException, MessageEOFException) ;
    virtual int readBytes(char* buffer, int length) throw (MessageNotReadableException) ;
    virtual bool readBoolean() throw (MessageNotReadableException, MessageEOFException) ;
    virtual double readDouble() throw (MessageNotReadableException, MessageEOFException) ;
    virtual float readFloat() throw (MessageNotReadableException, MessageEOFException) ;
    virtual int readInt() throw (MessageNotReadableException, MessageEOFException) ;
    virtual long long readLong() throw (MessageNotReadableException, MessageEOFException) ;
    virtual short readShort() throw (MessageNotReadableException, MessageEOFException) ;
    virtual p<string> readUTF() throw (MessageNotReadableException, MessageEOFException) ;
    virtual void writeBoolean(bool value) throw (MessageNotWritableException) ;
    virtual void writeByte(char value) throw (MessageNotWritableException) ;
    virtual void writeBytes(char* value, int length) throw (MessageNotWritableException) ;
    virtual void writeDouble(double value) throw (MessageNotWritableException) ;
    virtual void writeFloat(float value) throw (MessageNotWritableException) ;
    virtual void writeInt(int value) throw (MessageNotWritableException) ;
    virtual void writeLong(long long value) throw (MessageNotWritableException) ;
    virtual void writeShort(short value) throw (MessageNotWritableException) ;
    virtual void writeUTF(p<string> value) throw (MessageNotWritableException) ;

private:
    void expandBody() ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQBytesMessage_hpp_*/
