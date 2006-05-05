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
// ----------------------------------------------------------------------------
//
//      =====================================================================
//      Independent Framework (IFR) version 0.1 (BETA)  Stockholm, 2006-04-05
//      =====================================================================
//
//      IFR is a 100% header-only library. It does not require lib-file to
//      link with. It is licensed with the BSD license (see the license
//      above).
//
//      During this beta stage of IFR, the internal design of IFR may change
//      until version 1.0 has been released. When version 1.0 is out, the
//      internal structure is intended to become freezed. Version 1.x is
//      intended to be a binary contract defined by it's headers. Only
//      bugfixes that do not affect the internal structure should then be
//      made. New include files with new classes may be added though. The
//      aim is making it safe to bundle the IFR headers together with any
//      other library without risking a mix of versions with other libs that
//      also bundles IFR. The aim is also to make it safe to build a DLL/SO
//      API based on IFR 1.x without requiring rebuild of the binaries or
//      dynamic libraries when newer version of IFR comes out. The binary
//      contract defined by the IFR include headers must never break even
//      if the application and the module use different versions of IFR.
//
//      If the design or internal structure of IFR must be changed in
//      future, the future version will be placed under namespace ifr::v2
//      so that libs that bundles older version of IFR may coexist with
//      libs that bundles a newer version of IFR.
//
//      PUBLIC PART OF THE INTERFACE
//      ============================
//
//      Library users must only include files without the .hpp file
//      extension. Files with the .hpp file extension are not public. User's
//      code will break in future if including the hpp files directly. Only
//      headers without file extensions (#include <ifr/p>, #include
//      <ifr/array>) are public includes and will in the release version
//      contain all the content from the hpp files.
//
//      User of the library must not use classes within a "nonpublic"
//      namespace. All symbols defined within a namespace named "nonpublic"
//      may change and is only intended for use internally by IFR.
//
// ----------------------------------------------------------------------------
#ifndef IFR_V1_ATOMIC_HPP_
#define IFR_V1_ATOMIC_HPP_

#include "platform.hpp"

namespace ifr {
  namespace v1 {
    class atomic {
    private:
      // Invalidate copy constructor and assignment operator
      atomic (const atomic&);
      atomic& operator= (const atomic&);

#ifdef IFR_USE_PTHREAD
    protected:
      long counter_;
      pthread_mutex_t m_;
    public:
      atomic () : counter_ (0) {
        pthread_mutex_init(&m_, NULL);
      }
      atomic (long init_value) : counter_ (init_value) {
        pthread_mutex_init(&m_, NULL);
      }
      ~atomic () {
        pthread_mutex_destroy(&m_);
      }
      void increment() {
        pthread_mutex_lock(&m_);
        ++counter_;
        pthread_mutex_unlock(&m_);
      }
      bool decrement() {
        pthread_mutex_lock(&m_);
        if (!--counter_) {
          pthread_mutex_unlock(&m_);
          return true;
        } else {
          pthread_mutex_unlock(&m_);
          return false;
        }
      }
      long getvalue() const {
        pthread_mutex_lock(&m_);
        long retval = counter_;
        pthread_mutex_unlock(&m_);
        return retval;
      }
      /*void setvalue(long value) {
        pthread_mutex_lock(&m_);
        counter_ = value;
        pthread_mutex_unlock(&m_);
      }*/
      static const char* implementation () {
        return "pthread";
      }
#elif defined IFR_USE_GNU_ATOMIC
    protected:
      mutable _Atomic_word counter_;
    public:
      atomic () : counter_ (0) {}
      atomic (long init_value) : counter_ (init_value) {}
      inline void increment() {
        __atomic_add(&counter_, 1);
      }
      inline bool decrement() {
        return (__exchange_and_add(&counter_, -1) == 1);
      }
      inline long getvalue() const {
        return __exchange_and_add(&counter_, 0);
      }
      /*inline void setvalue(long value) {
        counter_ = value;
      }*/
      static const char* implementation () {
        return "gnu atomic";
      }
#elif defined IFR_USE_INTERLOCKED
    protected:
      mutable long counter_;
    public:
      atomic () : counter_ (0) {}
      atomic (long init_value) : counter_ (init_value) {}
      inline void increment() {
        _InterlockedIncrement (&counter_);
      }
      inline bool decrement() {
        return (_InterlockedDecrement (&counter_) == 0);
      }
      inline long getvalue() const {
        return _InterlockedExchangeAdd (&counter_, 0);
      }
      /*inline void setvalue(long value) {
        counter_ = value;
      }*/
      static const char* implementation () {
        return "windows interlocked";
      }
#else
#error "atomic Operations Unsupported"
#endif
    };
  }
}
#endif // IFR_V1_ATOMIC_HPP_

