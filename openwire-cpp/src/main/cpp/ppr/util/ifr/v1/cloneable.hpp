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
#ifndef IFR_V1_CLONEABLE_HPP_
#define IFR_V1_CLONEABLE_HPP_

#include "p.hpp"
namespace ifr {
  namespace v1 {
    /*  Deriving from this struct indicates that your class has the methods:

          YourClass* clone() const; // Performs shallow clone.
          YourClass* deepClone() const; // Performs deep clone.

        A deep clone means that the object and all it's members are cloned.
        A shallow clone means that only the object are cloned but not
        nescessarily it's members.

        However, you should normally create your cloning code in the copy
        constructor, which is the default cloning method in C++. But for
        classes that of some reason cannot use the copy constructor, the
        clone method is the alternative. Specifically if the new operator
        is protected for a class, then it is not possible to clone the
        object in a normal way, which is the case with some of the
        collection classes in IFR.

        Examples:

        #ifndef MYLIB_MYCLASS_HPP_
        #define MYLIB_MYCLASS_HPP_

        #include <ifr/array>

        namespace mylib {
          using namespace ifr;

          class MyClass : public cloneable
          {
          private:
            p<int> intMember_;
            array<unsigned char> blobMember_;
          public:
            p<MyClass> clone () {
              // Default clone - use new() and the default copy constructor:
              return new MyClass (*this);
            }
            p<MyClass> deepClone () {
              // Deep clone - create a copy of all members that are pointers:
              p<MyClass> retval = new MyClass ();
              if (intMember_ != NULL) {
                *retval->intMember_ = *intMember_;
              }
              if (blobMember_ != NULL) {
                *retval->blobMember_ = blobMember_.deepClone();
              }
              return retval;
            }
          };
        }
        #endif //MYLIB_MYCLASS_HPP_

        ----

        #include <ifr/array>
        #include <mylib/MyClass.hpp>

        using namespace ifr;
        using namespace mylib;

        int main () {
          array<MyClass> orig (10);
          array<MyClass> shallowClone = orig.clone(); // Copies pointers only
          array<MyClass> deepClone = orig.deepClone(); // Copies instances also

          return 0;
        }

    */
    struct cloneable {};

    /** Returns a clone of given object.
      If given object derives from struct cloneable, this function calls
      clone() on it.
      If given object does not derive from struct cloneable, this function
      calls the copy constructor of given object.
    */
    template <typename T> T* get_clone (const T& obj, const void*) {
      return new T (obj);
    }
    template <typename T> T* get_clone (const T& obj, const cloneable*) {
      return obj.clone();
    }
    /** Clones an instance.
      This function will call the copy constructor of given type unless given class is
      derived from clonable. If so, clone() is called on the object.

      For classes that do not derive from cloneable, the new operator +
      copy constructor defines the behavior of both clone() and deelClone().

      Examples:
        int origInt = 3;
        int clonedInt = clone (origInt);

        std::string origString = "Hello World";
        std::string clonedString = clone (origString);

        int* origInt = new int (3);
        int* clonedInt = clone (origInt);

        p<int> origInt = new int (3);
        p<int> clonedInt = clone (origInt);

      @param obj Object to clone.
      @param depth Clone depth. Only applied if the class is derived from cloneable.
        SHALLOW_CLONE (default) specifies that the instance will be cloned but not
        nescessarily it's members.
        DEEP_CLONE specifies that the instance as well as it's members should be cloned.
    */
    /*
    template <typename T> p<T> getclone (const T& obj) {
      return get_clone (obj, (const T*) NULL);
    }
    template <typename T> p<T> getdeepclone (const T& obj) {
      return get_deep_clone (obj, (const T*) NULL);
    }
    template <typename T> p<T> getclone (const p<const T>& obj) {
      return get_clone (*obj, (const T*) NULL);
    }
    template <typename T> p<T> getdeepclone (const p<const T>& obj) {
      return get_deep_clone (*obj, (const T*) NULL);
    }*/
  }
}

#endif // IFR_V1_CLONEABLE_HPP_
