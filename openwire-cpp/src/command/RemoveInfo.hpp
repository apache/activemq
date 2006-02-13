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
#ifndef RemoveInfo_hpp_
#define RemoveInfo_hpp_

#include "command/BaseCommand.hpp"
#include "command/IDataStructure.hpp"
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

/*
 * Dummy, should be auto-generated.
 */
class RemoveInfo : public BaseCommand
{
private:
    p<IDataStructure> objectId ;

public:
    const static int TYPE = 12 ;

public:
    RemoveInfo() ;
    virtual ~RemoveInfo() ;

    virtual p<IDataStructure> getObjectId() ;
    virtual void setObjectId(p<IDataStructure> oid) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*RemoveInfo_hpp_*/
