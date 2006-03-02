/*
 * Copyright (c) 2006, David Fahlander
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
#ifndef IFR_V1_AP_HPP_
#define IFR_V1_AP_HPP_

#include <new>
#include <cstdlib>
#include <cassert>
#include <util/ifr/p>

namespace ifr {
    namespace v1 {

template <typename T> class ap {
private:
  struct Array : refcounted {
    // Members
    size_t size_;
    p<T>* array_;
    // Construct
    Array (size_t size) : size_ (size) {
      array_ = (p<T>*) malloc (size * sizeof(p<T>));
      if (array_ == NULL) {
        throw std::bad_alloc ();
      }
      memset (array_, 0, sizeof (p<T>) * size);
    }
    virtual ~Array () {
      for (size_t i=0; i<size_; ++i) {
        if (array_[i] != NULL) {
          smartptr_release (reinterpret_cast<T*>(p_help::get_obj (array_[i])), reinterpret_cast<T*> (NULL));
        }
      }
      free (array_);
    }
  };
  p<Array> a;
public:
  ap () {}
  ap& operator = (class NullClass* nullclass) {
    assert (nullclass == NULL);
    a = NULL;
    return *this;
  }
  ap (size_t size) : a (new Array (size)) {}
  p<T>& operator [] (size_t pos) {
    return a->array_[pos];
  }
  const p<T>& operator [] (size_t pos) const {
    return array_[pos];
  }
  size_t size() const {
    return a->size_;
  }
};

  }
}

#endif // IFR_V1_AP_HPP_
