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
#include "util/Endian.hpp"

#if APR_IS_BIGENDIAN
  // Don't define
#else

float htonf( const float f )
{
    int i = htoni( *(int *)&f ) ;
    return *(float *)&i ;
}

float ntohf( const float f )
{
    int i = ntohi( *(int *)&f ) ;
    return *(float *)&i ;
}

double htond( const double d )
{
    long long l = htonl( *(long long *)&d ) ;
    return *(double *)&l ;
}

double ntohd( const double d )
{
    long long l = ntohl( *(long long *)&d ) ;
    return *(double *)&l ;
}

#endif
