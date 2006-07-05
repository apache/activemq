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
#ifndef ActiveMQ_BaseCommand_hpp_
#define ActiveMQ_BaseCommand_hpp_

#include <string>
#include "activemq/ICommand.hpp"
#include "activemq/command/BaseDataStructure.hpp"
#include "activemq/protocol/IMarshaller.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
#include "ppr/io/IOException.hpp"
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

/*
 * 
 */
class BaseCommand : public BaseDataStructure, public ICommand
{
protected:
    int  commandId ;
    bool responseRequired ;

public:
    virtual int getCommandId() ;
    virtual void setCommandId(int id) ;
    virtual bool getResponseRequired() ;
    virtual void setResponseRequired(bool value) ;

    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw(IOException) ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw(IOException) ;

    // Equals operator
    bool operator== (BaseCommand& other) ;

    virtual int getHashCode() ;
    virtual p<string> toString() ;
};

/* namespace */
    }
  }
}

#endif /*ActiveMQ_BaseCommand_hpp_*/
