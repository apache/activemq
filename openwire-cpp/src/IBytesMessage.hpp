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
#ifndef IBytesMessage_hpp_
#define IBytesMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include "IMessage.hpp"
#include "MessageEOFException.hpp"
#include "MessageNotReadableException.hpp"
#include "MessageNotWritableException.hpp"
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
        using namespace std;
        using namespace ifr::v1;

/*
 * 
 */
class IBytesMessage //: IMessage
{
public:

	virtual ~IBytesMessage(){}
    virtual int getBodyLength() = 0;
    virtual void reset() = 0 ;
    virtual char readByte() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual int readBytes(char* buffer, int length) throw (MessageNotReadableException) = 0 ;
    virtual bool readBoolean() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual double readDouble() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual float readFloat() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual int readInt() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual long long readLong() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual short readShort() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual p<string> readUTF() throw (MessageNotReadableException, MessageEOFException) = 0 ;
    virtual void writeBoolean(bool value) throw (MessageNotWritableException) = 0 ;
    virtual void writeByte(char value) throw (MessageNotWritableException) = 0 ;
    virtual void writeBytes(char* value, int length) throw (MessageNotWritableException) = 0 ;
    virtual void writeDouble(double value) throw (MessageNotWritableException) = 0 ;
    virtual void writeFloat(float value) throw (MessageNotWritableException) = 0 ;
    virtual void writeInt(int value) throw (MessageNotWritableException) = 0 ;
    virtual void writeLong(long long value) throw (MessageNotWritableException) = 0 ;
    virtual void writeShort(short value) throw (MessageNotWritableException) = 0 ;
    virtual void writeUTF(p<string> value) throw (MessageNotWritableException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*IBytesMessage_hpp_*/
