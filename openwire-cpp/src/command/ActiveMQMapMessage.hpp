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
#ifndef ActiveMQMapMessage_hpp_
#define ActiveMQMapMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <map>
#include <string>
#include "ActiveMQMessage.hpp"
#include "IllegalArgumentException.hpp"
#include "MessageFormatException.hpp"
#include "util/ifr/ap"
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
        using namespace ifr;
        using namespace apache::activemq::client::util;

/*
 * 
 */
class ActiveMQMapMessage : public ActiveMQMessage
{
private:
    p<string> text ;
    map< p<string>, p<MapItemHolder> > contentMap ;

public:
    const static int TYPE = 25 ;

public:
    ActiveMQMapMessage() ;
    virtual ~ActiveMQMapMessage() ;

    virtual int getCommandType() ;

    virtual bool getBoolean(const char* name) throw (MessageFormatException) ;
    virtual void setBoolean(const char* name, bool value) throw (IllegalArgumentException) ;
    virtual char getByte(const char* name) throw (MessageFormatException) ;
    virtual void setByte(const char* name, char value) throw (IllegalArgumentException) ;
    virtual ap<char> getBytes(const char* name) throw (MessageFormatException) ;
    virtual void setBytes(const char* name, ap<char> value) throw (IllegalArgumentException) ;
    virtual double getDouble(const char* name) throw (MessageFormatException) ;
    virtual void setDouble(const char* name, double value) throw (IllegalArgumentException) ;
    virtual float getFloat(const char* name) throw (MessageFormatException) ;
    virtual void setFloat(const char* name, float value) throw (IllegalArgumentException) ;
    virtual int getInt(const char* name) throw (MessageFormatException) ;
    virtual void setInt(const char* name, int value) throw (IllegalArgumentException) ;
    virtual long long getLong(const char* name) throw (MessageFormatException) ;
    virtual void setLong(const char* name, long long value) throw (IllegalArgumentException) ;
    virtual short getShort(const char* name) throw (MessageFormatException) ;
    virtual void setShort(const char* name, short value) throw (IllegalArgumentException) ;
    virtual p<string> getString(const char* name) throw (MessageFormatException) ;
    virtual void setString(const char* name, p<string> value) throw (IllegalArgumentException) ;
    virtual ap<string> getMapNames() ;
    virtual bool itemExists(const char* name) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQMapMessage_hpp_*/