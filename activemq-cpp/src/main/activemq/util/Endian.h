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
#ifndef ACTIVEMQ_UTIL_ENDIAN_H
#define ACTIVEMQ_UTIL_ENDIAN_H

#if defined( unix ) || defined(__APPLE__)
#include <netinet/in.h>
#else
#include <Winsock2.h>
#endif

// First try - check __BYTE_ORDER macro
#if !defined IFR_IS_BIG_ENDIAN && !defined IFR_IS_LITTLE_ENDIAN && !defined IFR_IS_DPD_ENDIAN
# if defined( unix ) || defined(__APPLE__)
#  include <sys/param.h> // defines __BYTE_ORDER (or sometimes __LITTLE_ENDIAN or __BIG_ENDIAN or __PDP_ENDIAN)
# endif
# if defined (__GLIBC__)
#  include <endian.h> // Can also define __BYTE_ORDER
# endif
# ifdef __BYTE_ORDER
#  if __BYTE_ORDER == __LITTLE_ENDIAN
#   define IFR_IS_LITTLE_ENDIAN
#  elif __BYTE_ORDER == __BIG_ENDIAN
#   define IFR_IS_BIG_ENDIAN
#  elif __BYTE_ORDER == __PDP_ENDIAN
#   define IFR_IS_PDP_ENDIAN
#  endif
# endif
#endif

// Second try - check __LITTLE_ENDIAN or __BIG_ENDIAN
#if !defined IFR_IS_BIG_ENDIAN && !defined IFR_IS_LITTLE_ENDIAN && !defined IFR_IS_DPD_ENDIAN
# if defined __LITTLE_ENDIAN
#  define IFR_IS_LITTLE_ENDIAN
# elif defined __BIG_ENDIAN
#  define IFR_IS_BIG_ENDIAN
# elif defined __PDP_ENDIAN
#  define IFR_IS_PDP_ENDIAN
# endif
#endif

// Last try - find out from well-known processor types using little endian
#if !defined IFR_IS_BIG_ENDIAN && !defined IFR_IS_LITTLE_ENDIAN && !defined IFR_IS_DPD_ENDIAN
# if defined (i386) || defined (__i386__) \
  || defined (_M_IX86) || defined (vax) \
  || defined (__alpha) || defined (__alpha__) \
  || defined (__x86_64__) || defined (__ia64) \
  || defined (__ia64__) || defined (__amd64__) \
  || defined (_M_IX86) || defined (_M_IA64) \
  || defined (_M_ALPHA)
#  define IFR_IS_LITTLE_ENDIAN
# else
#  if defined (__sparc) || defined(__sparc__) \
  || defined(_POWER) || defined(__powerpc__) \
  || defined(__ppc__) || defined(__hppa) \
  || defined(_MIPSEB) || defined(_POWER) \
  || defined(__s390__)
#   define IFR_IS_BIG_ENDIAN
#  endif
# endif
#endif

// Show error if we still don't know endianess
#if !defined IFR_IS_BIG_ENDIAN && !defined IFR_IS_LITTLE_ENDIAN && !defined IFR_IS_DPD_ENDIAN
#error "Could not determine endianess of your processor type"
#endif

// Use these if the compiler does not support _intXX
#ifdef NEEDS_INT_DEFINED
#define _int16 short
#define _int32 int
#define _int64 long long
#endif

// Check for uintXX types
#ifndef uint8_t
#define uint8_t unsigned char
#endif
#ifndef uint16_t
#define uint16_t unsigned short
#endif
#ifndef uint32_t
#define uint32_t unsigned int
#endif
#ifndef uint64_t
#define uint64_t unsigned long long
#endif

// Macros and helpers for endian conversion
namespace activemq{
namespace util{
	
/*#ifdef IFR_IS_BIGENDIAN
inline unsigned int       htoni   (unsigned int i)        { return i; }
inline unsigned long long htonll  (unsigned long long ll) { return ll; }
inline float              htonf   (float f)               { return f; }
inline double             htond   (double d)              { return d; }
inline unsigned int       ntohi   (unsigned int i)        { return i; }
inline unsigned long long ntohll  (unsigned long long ll) { return ll; }
inline float              ntohf   (float f)               { return f; }
inline double             ntohd   (double d)              { return d; }
#else // !IFR_IS_BIGENDIAN

inline unsigned int htoni (unsigned int i) {
  return ( i << 8  ) & 0xFF00 |
	     ( i >> 8  ) & 0x00FF;
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
  unsigned int i = htonl( *(unsigned int *)&f ) ;
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
*/
	class Endian{
	public:
	
		static void byteSwap(unsigned char* data, int dataLength) {
			
			#ifdef IFR_IS_BIGENDIAN
				return;
			#endif
			
		    for (int i = 0; i<dataLength/2; i++) {
				unsigned char temp = data[i];
				data[i] = data[dataLength-1-i];
				data[dataLength-1-i] = temp;
		    }
		}
		
		static uint8_t byteSwap( uint8_t value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
		
		static uint16_t byteSwap( uint16_t value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
		
		static uint32_t byteSwap( uint32_t value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
		
		static uint64_t byteSwap( uint64_t value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
		
		static float byteSwap( float value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
		
		static double byteSwap( double value ){
			byteSwap( (unsigned char*)&value, sizeof( value ) );
			return value;
		}
	};
	
//#endif // IFR_IS_BIGENDIAN

}}

#endif /*ACTIVEMQ_UTIL_ENDIAN_H*/
