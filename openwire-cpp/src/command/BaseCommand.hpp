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
#ifndef BaseCommand_hpp_
#define BaseCommand_hpp_

#include <string>
#include "command/AbstractCommand.hpp"
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr::v1;
        using namespace std;

/*
 * 
 */
class BaseCommand : public AbstractCommand
{
private:
    int  commandId ;
    bool responseRequired ;

public:
    const static int TYPE = 0 ;

public:
    BaseCommand() ;
    virtual ~BaseCommand() ;

    // Equals operator
    bool operator== (BaseCommand& other) ;

    virtual int getHashCode() ;
    virtual int getCommandType() ;
    virtual int getCommandId() ;
    virtual void setCommandId(int id) ;
    virtual bool isResponseRequired() ;
    virtual void setResponseRequired(bool required) ;
    virtual p<string> toString() ;
};

/* namespace */
      }
    }
  }
}

#endif /*BaseCommand_hpp_*/
