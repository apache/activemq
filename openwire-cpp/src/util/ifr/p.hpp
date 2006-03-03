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
#ifndef IFR_V1_P_HPP_
#define IFR_V1_P_HPP_

#include "atomic.hpp"

namespace ifr {
  namespace v1 {
    // Forward declaration of p
    template <typename T> class p;

    // This is an internal class that helps getting the private member obj_ from p.
    // The normal thing would be to use "friend" but that is not possible since
    // there are template methods and classes that must be friends.
    struct p_help {
      template <typename T> static void* get_obj (const p<T>& sp) {
        return sp.obj_;
      }
    };

    /** Generic thread-safe reference counted smart pointer for any type.
    */
    template <typename T> class p {
      friend struct p_help;
    private: // Should be private. But public because p<A> cannot be friend of p<B> on GCC compiler.
      void* obj_;
    public:
      /** Default constructor. Initiates pointer to zero.
      */
      p () : obj_ (0) {}
      /** Constructor taking ownership of given pointer. Caller looses the ownership of given object.
      */
      p (T* obj) {
        if (obj) {
          obj_ = smartptr_assign (reinterpret_cast<T*>(obj), reinterpret_cast<T*>(NULL));
        } else {
          obj_ = NULL;
        }
      }
      p (T*& obj) {
        // This constructor is not ment to compile!
        // It is here to generate an error when trying to assign a p<T> from
        // an lvalue. p<T> must only be assigned directly from the return value
        // of the new() operator.
        //
        // Compiler will not complain about the error text below, but will complain
        // about "ambigous call to overloaded function"...
        ERROR_CANNOT_ASSIGN_P_FROM_LVALUE(obj);
      }

      /** Destructor. Decrements reference count and eventually deletes the object.
      */
      ~p () {
        if (obj_) smartptr_release (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
      }
      /** Copy Constructor. Increments reference count since we also own the object now.
      */
      p (const p& other) {
        obj_ = other.obj_;
        if (obj_) smartptr_addref (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
      }
      /** Copy Constructor for p of compatible class.
        Increments reference count since we also own the object now.
      */
      template <class DerivedClass> p (const p<DerivedClass>& other) {
        obj_ = smartptr_convert(
          (T*)NULL,
          (T*)NULL,
          reinterpret_cast<DerivedClass*>(p_help::get_obj(other)),
          (DerivedClass*)NULL);
        if (obj_) smartptr_addref (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
      }
      /** Assignment operator. Increments reference count since we also own the object now.
          Decrements refcount of the old object because we don't own that anymore.
      */
      p& operator = (const p& other) {
        if (other.obj_) smartptr_addref (reinterpret_cast<T*>(other.obj_), reinterpret_cast<T*>(NULL));
        if (obj_) smartptr_release (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
        obj_ = other.obj_;
        return *this;
      }
      /** Assignment operator for p of compatible class.
        Increments reference count since we also own the object now. Decrements refcount
        of the old object because we don't own that anymore.
      */
      template <class DerivedClass> p& operator = (const p<DerivedClass>& other) {
        if (p_help::get_obj(other)) smartptr_addref (reinterpret_cast<DerivedClass*>(p_help::get_obj(other)), reinterpret_cast<DerivedClass*>(NULL));
        if (obj_) smartptr_release (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
        obj_ = smartptr_convert((T*)NULL, (T*)NULL, reinterpret_cast<DerivedClass*>(p_help::get_obj(other)), (DerivedClass*)NULL);
        return *this;
      }
      /** Assignment operator taking ownership of given pointer.
        Caller looses the ownership of given object. Decrements refcount
        of the old object because we don't own that anymore.
      */
      p& operator = (T* obj) {
        if (obj_) smartptr_release (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
        if (obj) {
          obj_ = smartptr_assign (reinterpret_cast<T*>(obj), reinterpret_cast<T*>(NULL));
        } else {
          obj_ = NULL;
        }
        return *this;
      }
      p& operator = (T*& obj) {
        // This constructor is not ment to compile!
        // It is here to generate an error when trying to assign a p<T> from
        // an lvalue. p<T> must only be assigned directly from the return value
        // of the new() operator.
        //
        // Compiler will not complain about the error text below, but will complain
        // about "ambigous call to overloaded function"...
        ERROR_CANNOT_ASSIGN_P_FROM_LVALUE(obj);
        return *this;
      }
      /** Member reach operator.
      */
      T* operator -> () const {return smartptr_get (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));}
      /** Reference reach operator. 
      */
      T& operator * () const {
        return *smartptr_get (reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL));
      }
      /** Equal Operator. Compares the container's pointer value. Used in order to insert the p in std::maps and std::set.
      */
      bool operator == (const p& other) const {return obj_ == other.obj_;}
      /** Equal Operator. Compares the pointer value. Used mainly to compare with NULL.
      */
      bool operator == (const T* other) const {
        return smartptr_get(reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL)) == other;
      }
      /** No-Equal Operator. Compares the container's pointer value. Used in order to insert the p in std collections.
      */
      bool operator != (const p& other) const {return obj_ != other.obj_;}
      /** No-Equal Operator. Compares the pointer value. Used mainly to compare with NULL.
      */
      bool operator != (const T* other) const {
        return smartptr_get(reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL)) != other;
      }
      /** Less Operator. Compares the container's pointer value. Used in order to insert the p in std collections.
      */
      bool operator < (const p& other) const {
        return smartptr_get(reinterpret_cast<T*>(obj_), reinterpret_cast<T*>(NULL)) <
               smartptr_get(reinterpret_cast<T*>(other.obj_), reinterpret_cast<T*>(NULL));
      }
    };

    /**
    */
    template <typename T> long get_refcount (const p<T>& ptr) {
      return smartptr_get_refcount (reinterpret_cast<const T*>(p_help::get_obj(ptr)), (const T*) NULL);
    }

    /**
    */
    template <typename T> const T* getptr (const p<T>& ptr) {
      return smartptr_get (reinterpret_cast<T*>(p_help::get_obj(ptr)), (T*) NULL);
    }

    /**
    */
    template <typename T> T* getptr (p<T>& ptr) {
      return smartptr_get (reinterpret_cast<T*>(p_help::get_obj(ptr)), (T*) NULL);
    }

    /**
    */
    template <typename T> p<T> encapsulate (T* ptr) {
      return smartptr_encapsulate (ptr, (T*) NULL);
    }

    /**
    */
    template <typename T> struct PointerHolder {
      atomic refcount_;
      T* obj_;
      PointerHolder (T* obj, long initcount) : refcount_ (initcount), obj_ (obj) {}
      void addref () {
        refcount_.increment();
      }
      void release () {
        if (refcount_.decrement()) {
          delete obj_;
          delete this;
        }
      }
    };


    /* Overloaded smartptr functions for void
    */
    template <typename T> void* smartptr_assign (T* obj, void*) {
      return new PointerHolder<T> (obj, 1);
    }
    template <typename T> void smartptr_addref (T* obj, void*) {
      reinterpret_cast<PointerHolder<T>*>(obj)->refcount_.increment();
    }
    template <typename T> void smartptr_release (T* obj, void*) {
      reinterpret_cast<PointerHolder<T>*>(obj)->release();
    }
    template <typename T> T* smartptr_get (T* obj, void*) {
      return (obj ? reinterpret_cast<PointerHolder<T>*>(obj)->obj_ : NULL);
    }
    template <typename Base, typename Derived> Base* smartptr_convert (Base*, void*, Derived* obj, void*) {
      return obj;
    }
    template <typename T> long smartptr_get_refcount (const T* obj, const void*) {
      return reinterpret_cast<const PointerHolder<T>*>(obj)->refcount_.get_value();
    }
    template <typename T> T* smartptr_encapsulate (T* obj, void*) {
      // This constructor is not ment to compile!
      // It is here to generate an error when trying to call encapsulate() on
      // an instance of a class that is not possible to re-encapsulate into a smart pointer.
      // This error can occur if the class does not derive from recounted, IUnknown
      // or any other class that supports re-encapsulation into a smart pointer.
      ERROR_TYPE_NOT_POSSIBLE_TO_ENCAPSULATE(obj);
    }


    /** Base class that, when derived from, makes your class
        * Faster to assign to a p<your class>.
        * Faster to delete when your refcount reaches zero.
        * Possible to use with encapsulate().
    */
    class refcounted {
      friend void addref (refcounted*);
      friend void release (refcounted*);
      friend long get_refcount (const refcounted*);
    private:
      atomic refcount_;
    public:
      refcounted() : refcount_ (1) {}
      virtual ~refcounted() {}
    };

    inline void addref (refcounted* o) {
      o->refcount_.increment();
    }
    inline void release (refcounted* o) {
      if (o->refcount_.decrement()) delete o;
    }
    inline long get_refcount (const refcounted* o) {
      return o->refcount_.get_value();
    }

    /* Overloaded smartptr functions for refcounted
    */
    template <typename T> T* smartptr_assign (T* obj, refcounted*) {
      return obj;
    }
    template <typename T> void smartptr_addref (T* obj, refcounted*) {
      addref (obj);
    }
    template <typename T> void smartptr_release (T* obj, refcounted*) {
      release (obj);
    }
    template <typename T> T* smartptr_get (T* obj, refcounted*) {
      return obj;
    }
    template <typename Base, typename Derived> Base* smartptr_convert (Base*, refcounted*, Derived* obj, refcounted*) {
      return obj;
    }
    template <typename Base, typename Derived> Base* smartptr_convert (Base*, void*, Derived* obj, refcounted*) {
      // This overloaded method must not compile.
      // It is here to generate an error when trying to mix instances of refcounted and non-refcounted.
      // If you get a compilation error here, you should derive from refcount already in given
      // base class and not only in given subclass.
      // If that is not possible, your subclass must not derive from refcounted.
      ERROR_CANNOT_ASSIGN_REFCOUNTED_INSTANCE_TO_NON_REFCOUNTED_BASE((Base*) NULL, obj);
      return NULL;
    }
    template <typename T> long smartptr_get_refcount (const T* obj, const refcounted*) {
      return get_refcount (obj);
    }
    template <typename T> T* smartptr_encapsulate (T* obj, refcounted*) {
      addref(obj);
      return obj;
    }
  }
}

/** IUnknown - Makes p<> hold COM objects according to it's standard.
*/
struct IUnknown;

namespace bun {
  namespace v1 {
    /* Overloaded smartptr functions for IUnknown
    */
    template <typename T> T* smartptr_assign (T* obj, IUnknown*) {
      return obj;
    }
    template <typename T> void smartptr_addref (T* obj, IUnknown*) {
      obj->AddRef();
    }
    template <typename T> void smartptr_release (T* obj, IUnknown*) {
      obj->Release();
    }
    template <typename T> T* smartptr_get (T* obj, IUnknown*) {
      return obj;
    }
    template <typename Base, typename Derived> Base* smartptr_convert (Base*, IUnknown*, Derived* obj, IUnknown*) {
      return obj;
    }
    template <typename Base, typename Derived> Base* smartptr_convert (Base*, void*, Derived* obj, IUnknown*) {
      // This overloaded method must not compile.
      // It is here to generate an error when trying to mix instances of IUnknown and non-IUnknown.
      // If you get a compilation error here, you should derive from IUnknown already in given
      // base class and not only in given subclass.
      // If that is not possible, your subclass must not derive from IUnknown.
      ERROR_CANNOT_ASSIGN_IUNKNOWN_INSTANCE_TO_NON_IUNKNOWN_BASE((Base*) NULL, obj);
      return NULL;
    }
    template <typename T> long smartptr_get_refcount (const T* obj, const IUnknown*) {
      const_cast<T*>(obj)->AddRef();
      return (long) const_cast<T*>(obj)->Release();
    }
    template <typename T> T* smartptr_encapsulate (T* obj, IUnknown*) {
      obj->AddRef();
      return obj;
    }
  }
}

#endif // IFR_V1_P_HPP_
