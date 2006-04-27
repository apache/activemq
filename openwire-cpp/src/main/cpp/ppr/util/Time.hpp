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
#ifndef Ppr_Time_hpp_
#define Ppr_Time_hpp_

#ifdef unix
#include <sys/time.h>
#else
#include <time.h>
#include <sys/timeb.h>
#endif

namespace apache
{
  namespace ppr
  {
    namespace util
    {

/*
 * Helper class with time functions.
 */
class Time
{
private:
    Time() ;

public:
    ~Time() ;

    static long long getCurrentTimeMillis() ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_Time_hpp_*/
