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
#ifndef SessionInfo_hpp_
#define SessionInfo_hpp_

#include "command/BaseCommand.hpp"
#include "command/SessionId.hpp"
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
class SessionInfo : public BaseCommand
{
private:
    p<SessionId> sessionId ;

public:
    const static int TYPE = 4 ;

public:
    SessionInfo() ;
    virtual ~SessionInfo() ;

    virtual p<SessionId> getSessionId() ;
    virtual void setSessionId(p<SessionId> sessionId) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*SessionInfo_hpp_*/
