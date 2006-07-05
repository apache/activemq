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
#ifndef Ppr_Endian_hpp_
#define Ppr_Endian_hpp_

#ifdef unix
#include <netinet/in.h>
#else
#include <Winsock2.h>
#endif
#include "ppr/util/ifr/endian"

// Use these if the compiler does not support _intXX
#ifdef NEEDS_INT_DEFINED
#define _int16 short
#define _int32 int
#define _int64 long long
#endif

namespace apache
{
  namespace ppr
  {
    namespace util
    {

// Macros and helpers for endian conversion
#ifdef IFR_IS_BIG_ENDIAN
inline unsigned int       htoni   (unsigned int i)        { return i; }
inline unsigned long long htonll  (unsigned long long ll) { return ll; }
inline float              htonf   (float f)               { return f; }
inline double             htond   (double d)              { return d; }
inline unsigned int       ntohi   (unsigned int i)        { return i; }
inline unsigned long long ntohll  (unsigned long long ll) { return ll; }
inline float              ntohf   (float f)               { return f; }
inline double             ntohd   (double d)              { return d; }
#else // !IFR_IS_BIG_ENDIAN

inline unsigned int htoni (unsigned int i) {
  return
    ( i << 24 ) & 0xFF000000 |
	( i << 8  ) & 0x00FF0000 |
	( i >> 8  ) & 0x0000FF00 |
	( i >> 24 ) & 0x000000FF;
}

inline unsigned long long htonll (unsigned long long ll) {
  return
    ( ll << 56 ) & 0xFF00000000000000ULL |
	( ll << 40 ) & 0x00FF000000000000ULL |
	( ll << 24 ) & 0x0000FF0000000000ULL |
	( ll << 8  ) & 0x000000FF00000000ULL |
    ( ll >> 8  ) & 0x00000000FF000000ULL |
	( ll >> 24 ) & 0x0000000000FF0000ULL |
	( ll >> 40 ) & 0x000000000000FF00ULL |
	( ll >> 56 ) & 0x00000000000000FFULL;
}
inline float htonf (float f) {
  unsigned int i = htoni( *(unsigned int *)&f ) ;
  return *(float *)&i ;
}
inline double htond (double d) {
  unsigned long long ll = htonll( *(unsigned long long *)&d ) ;
  return *(double *)&ll ;
}
inline unsigned int       ntohi   (unsigned int i)        { return htoni (i); }
inline unsigned long long ntohll  (unsigned long long ll) { return htonll (ll); }
inline float              ntohf   (float f)               { return htonf (f); }
inline double             ntohd   (double d)              { return htond (d); }

#endif // IFR_IS_BIG_ENDIAN

/* namespace */
    }
  }
}

#endif /*Ppr_Endian_hpp_*/
