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
#ifndef BrokerId_hpp_
#define BrokerId_hpp_

#include <string>
#include "command/AbstractCommand.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr;
        using namespace std ;

/*
 *
 */
class BrokerId : public AbstractCommand
{
private:
    p<string> value ;

public:
    const static char TYPE = 124 ;

public:
    BrokerId() ;
    virtual ~BrokerId() ;

    virtual int getCommandType() ;
    virtual p<string> getValue() ;
    virtual void setValue(const char* brokerId) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*BrokerId_hpp_*/