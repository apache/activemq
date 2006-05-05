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
#ifndef Ppr_Guid_hpp_
#define Ppr_Guid_hpp_

#include <string>
#include <ppr/util/ifr/p>

#ifdef unix
#include <sys/param.h>
#endif
#include <stdio.h>
#include <assert.h>
#if defined(WIN32) || defined(__CYGWIN__)
#include <objbase.h>
#else
#include <uuid/uuid.h>
#endif

namespace apache
{
  namespace ppr
  {
    namespace util
    {
      using namespace std;
      using namespace ifr;

/*
 * Helper class that generates global unique identifiers.
 */
class Guid
{
private:
    Guid() ; // This class shall never be instanciated.

public:
    static unsigned char* getGuid() ;
    static p<string> getGuidString() ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_Guid_hpp_*/
