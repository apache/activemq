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
#ifndef Endian_hpp_
#define Endian_hpp_

#include <apr.h>

// Use these if the compiler does not support _intXX
#ifdef NEEDS_INT_DEFINED
#define _int16 short
#define _int32 int
#define _int64 long long
#endif

// Macros and helpers for endian conversion
#if APR_IS_BIGENDIAN
#if !defined(htons) 
  #define htons(x) x
#endif
#if !defined(htoni) 
  #define htoni(x) x
#endif
#if !defined(htonl) 
  #define htonl(x) x
#endif
#if !defined(htonf) 
  #define htonf(x) x
#endif
#if !defined(htond) 
  #define htond(x) x
#endif
#if !defined(ntohs) 
  #define ntohs(x) x
#endif
#if !defined(ntohi) 
  #define ntohi(x) x
#endif
#if !defined(ntohl) 
  #define ntohl(x) x
#endif
#if !defined(ntohf) 
  #define ntohf(x) x
#endif
#if !defined(ntohd) 
  #define ntohd(x) x
#endif
#else
#define htons(x) \
	( x << 8  ) & 0xFF00 | \
	( x >> 8  ) & 0x00FF
#define htoni(x) \
    ( x << 24 ) & 0xFF000000 | \
	( x << 8  ) & 0x00FF0000 | \
	( x >> 8  ) & 0x0000FF00 | \
	( x >> 24 ) & 0x000000FF
#define htonl(x) \
    ( x << 56 ) & 0xFF00000000000000LL | \
	( x << 40 ) & 0x00FF000000000000LL | \
	( x << 24 ) & 0x0000FF0000000000LL | \
	( x << 8  ) & 0x000000FF00000000LL | \
    ( x >> 8  ) & 0x00000000FF000000LL | \
	( x >> 24 ) & 0x0000000000FF0000LL | \
	( x >> 40 ) & 0x000000000000FF00LL | \
	( x >> 56 ) & 0x00000000000000FFLL
#define ntohs  htons
#define ntohi  htoni
#define ntohl  htonl

extern float htonf( const float f ) ;
extern float ntohf( const float f ) ;
extern double htond( const double d ) ;
extern double ntohd( const double d ) ;

#endif

#endif /*Endian_hpp_*/
