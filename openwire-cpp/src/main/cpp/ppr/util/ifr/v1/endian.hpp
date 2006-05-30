/*
 * Copyright (c) 2005-2006, David Fahlander
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. Neither the name of slurk.org nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// First try - check __BYTE_ORDER macro
#if !defined IFR_IS_BIG_ENDIAN && !defined IFR_IS_LITTLE_ENDIAN && !defined IFR_IS_DPD_ENDIAN
# ifdef unix
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

