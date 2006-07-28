/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ppr/util/Time.hpp"

using namespace apache::ppr::util;

/*
 *
 */
Time::Time()
{
    // no-op
}

/*
 *
 */
Time::~Time()
{
    // no-op
}

/*
 * Returns number of milliseconds since 1 Jan 1970 (UTC).
 */
long long Time::getCurrentTimeMillis()
{
    long long millis ;

#ifdef unix
    struct timeval tv ;
    struct timezone tz ;

    gettimeofday(&tv, &tz) ;

    millis  = tv.tv_sec ;
    millis += tv.tv_usec / 1000 ;
#else
    __time64_t ltime ;
    struct __timeb64 tstruct ;

    _time64( &ltime ) ;
    _ftime64( &tstruct ) ;

    millis  = ltime * 1000 ;
    millis += tstruct.millitm ;
#endif
    return  millis ;
}
