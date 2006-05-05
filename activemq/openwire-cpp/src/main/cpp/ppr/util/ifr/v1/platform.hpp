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
#ifndef IFR_V1_PLATFORM_HPP_
#define IFR_V1_PLATFORM_HPP_

// GCC
#if defined __GNUC__ 
#ifndef STDCALL
#define STDCALL       __attribute__ ((stdcall))
#endif
// Microsoft Visual C++
#elif defined _MSC_VER
#define STDCALL       __stdcall
// Disable warning C4290: C++ exception specification ignored except to 
//                        indicate a function is not __declspec(nothrow).
#pragma warning( disable : 4290) 
#endif

#if !defined (_WIN64) && defined (WIN64)
#define _WIN64
#endif

#if !defined (_WIN32) && defined (WIN32)
#define _WIN32
#endif

#if !defined (_WINDOWS) && (defined (_WIN32) || defined (_WIN64))
#define _WINDOWS
#endif

#if (defined __GNUC__)
#define IFR_USE_GNU_ATOMIC
#include <bits/c++config.h>
#include <bits/atomicity.h>
#ifdef __GLIBCXX__
using __gnu_cxx::__atomic_add;
using __gnu_cxx::__exchange_and_add;
#endif
#elif (!defined __GNUC__) && ( defined _WINDOWS ) // Non-GCC compiler, windows platform:
#define IFR_USE_INTERLOCKED
#else // Compiler or processor for which we got no atomic count support
#define IFR_USE_PTHREAD
#include <pthread.h>
#endif

#ifdef IFR_MODULARITY
// When defined, each DLL/SO-file is reference counted and released
// first when it does not own any object anymore.
#define IFR_REFCOUNT_LIBRARIES
// When defined, all deallocation (delete) will be performed by the same DLL/SO-file
// that did allocate the object (new).
#define IFR_DIFFERENT_HEAPS_AWARE
#endif

#ifdef _WINDOWS

// Defining Critical Section, needed for windows critical section handling.
// We dont want to include windows.h since it contains TOO much dirt.
extern "C" {
  #ifdef __GNUC__
  // GNU C Compiler
  struct _CRITICAL_SECTION;
  typedef struct _CRITICAL_SECTION CRITICAL_SECTION;
  #else
  // Other Windows Compilers. Same structure as in windows.h
  struct _RTL_CRITICAL_SECTION;
  typedef struct _RTL_CRITICAL_SECTION CRITICAL_SECTION;
    #ifdef _MSC_VER
    // Interlocked functions. Only for MSVC Compiler. GCC uses inline assembler.
    // Todo: Support Borland and other c++ compilers on windows with either inline
    // assember or using Interlocked functions.
    long __cdecl _InterlockedIncrement(volatile long*);
    long __cdecl _InterlockedDecrement(volatile long*);
    long __cdecl _InterlockedExchangeAdd (volatile long*, long);
    #pragma intrinsic( _InterlockedIncrement )
    #pragma intrinsic( _InterlockedDecrement )
    #pragma intrinsic( _InterlockedExchangeAdd )
    #endif
  #endif
  // Declare CriticalSection functions:
  __declspec(dllimport) void STDCALL InitializeCriticalSection (CRITICAL_SECTION*);
  __declspec(dllimport) void STDCALL EnterCriticalSection (CRITICAL_SECTION*);
  __declspec(dllimport) void STDCALL LeaveCriticalSection (CRITICAL_SECTION*);
  __declspec(dllimport) void STDCALL DeleteCriticalSection (CRITICAL_SECTION*);

  namespace ifr {
    namespace v1 {
      namespace windows {
        struct CriticalSection {
          void* DebugInfo;
          long LockCount;
          long RecursionCount;
          void* OwningThread;
          void* LockSemaphore;
          unsigned long SpinCount;
          inline operator CRITICAL_SECTION* () {
            return reinterpret_cast<CRITICAL_SECTION*>(this);
          }
        };
      }
    }
  }
}
#endif

#endif // IFR_V1_PLATFORM_HPP_

