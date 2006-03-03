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

#ifdef BUN_USE_PTHREAD
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
      long get_value() const {
        pthread_mutex_lock(&m_);
        long retval = counter_;
        pthread_mutex_unlock(&m_);
        return retval;
      }
      static const char* implementation () {
        return "pthread";
      }
#elif defined BUN_USE_GNU_ATOMIC
    protected:
      mutable _Atomic_word counter_;
    public:
      atomic () : counter_ (0) {}
      atomic (long init_value) : counter_ (init_value) {}
      void increment() {
        __atomic_add(&counter_, 1);
      }
      bool decrement() {
        return (__exchange_and_add(&counter_, -1) == 1);
      }
      long get_value() const {
        return __exchange_and_add(&counter_, 0);
      }
      static const char* implementation () {
        return "gnu atomic";
      }
#elif defined BUN_USE_INTERLOCKED
    protected:
      mutable long counter_;
    public:
      atomic () : counter_ (0) {}
      atomic (long init_value) : counter_ (init_value) {}
      void increment() {
        _InterlockedIncrement (&counter_);
      }
      bool decrement() {
        return (_InterlockedDecrement (&counter_) == 0);
      }
      long get_value() const {
        return _InterlockedExchangeAdd (&counter_, 0);
      }
      static const char* implementation () {
        return "windows interlocked";
      }
#else
#error "Atomic Operations Unsupported"
#endif
    };
  }
}
#endif // IFR_V1_ATOMIC_HPP_

