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
/* TODOS:
    * Test that p<const T> works as expected.
*/
#ifndef IFR_V1_P_HPP_
#define IFR_V1_P_HPP_

#include <cassert>
#include <typeinfo>
#include "atomic.hpp"
#ifndef NULL
#define NULL 0
#endif
namespace ifr {
  namespace v1 {
    // This class is only used for comparing with NULL.
    class NullClass;

    /** Base class that enhances the use of the p<> template.
    
       The p<> template is compatible with any type. However, classes that
       derives from refcounted gets the following benefits:

       * It becomes faster to create and assign them to a p<> smart pointer.
       * It becomes faster for the p<> smart pointer to delete them when
         they are no longer references.
       * It becomes possible convert ordinary pointers back to
         p<> smart pointers.
       
       The p<> template inspects it's type to see whether it maintains the
       reference counting mechanism internally or not. By default, the p<>
       template will assume that it's type does not maintain such reference
       counting and will therefore create a container object that performs
       the reference counting. However, if the type is recognized as one
       that can perform reference counting internally, the p<> template
       will use the type's reference counting mechanisms and does not need
       to create a separate reference counting container for the pointer.

       refcounted is an example of a class that handles reference counting
       internally. By deriving from refcounted, your object will contain
       a "built-in" thread-safe reference counter and every time you call
       new() or delete() on your class, only your instance will be created
       or freed instead of also having to create a separate container
       object as well.

       Also have a look at struct Interface, which derives from refcounted
       and is intended to use if your class is subject for multiple
       inheritance.
       
       The internal reference counting gives us another benefit except
       that it does not have to create a container. Since the class has
       a built-in reference counter, it can be converted to a ordinary
       pointer and back again to a smart pointer. Classes that lacks of
       such an internal reference counter lacks the possibility to be
       converted back to a smart pointer from an ordinary pointer.

       Converting an ordinary pointer to a smart pointer should be done
       using the smartify() template function. This function will
       only compile if the given class is recognized as a class with
       it's own internal reference counting mechanism.

       If you already have your own base class or other 3rd party
       base class that has it's own reference counter, you can make
       the p<> template recognize that class in the same way that it
       recognizes refcounted. You can do that without changing
       either the IFR code nor the code of the base class that you
       want to be recognized. What you do is simply to add some
       overloaded versions of the smartptr_xxx() functions and make
       sure that the header file containing these overloaded functions
       are always included everywhere that you use the p<> template
       with that class. To see an example of writing such overloaded
       methods, see how IUnknown is made to recognized by the p<>
       template using it's overloaded versions of the smartptr_xxx()
       functions.

       If your class is intended to be used as one of multiple base classes,
       you must derive virtually from refcounted. Otherwise derived instances
       will result in having ambigous methods and multiple reference counters.

       Normally, multiple inheritance is useful when implementing interfaces.
       It is therefore recommended (for simplicity and clean design) that your
       interfaces derive from Interface rather than from this class directly.
       The struct Interface derives virtually from refcounted for you.

      @see Interface
    */
    class refcounted {
      friend inline void addref (refcounted*);
      friend inline void release (refcounted*);
      friend inline long getrefcount (const refcounted*);
    private:
      atomic refcount_;
    public:
      virtual ~refcounted() {}
    protected:
      virtual void destroy() {
        delete this;
      }
    public:
      inline refcounted(long refcount) : refcount_ (refcount) {}
      inline refcounted() : refcount_ (1) {}
      // When a class derived from refcounted is copied, always reset refcount.
      inline refcounted(const refcounted&) : refcount_ (1) {}
      // When a class derived from refcounted is assigned, don't copy refcount.
      inline refcounted& operator = (const refcounted&) {return *this;}
    };

    /** Base class for interfaces.

      Structs (or classes) that derive from Interface get the following benefits:

        * They (and their children) does not need to define their own virtual
          destructor.
        * They (and their children) can convert their this-pointers to smart
          pointers by calling smartify(this).
        * Smart pointers of these classes or structs become more heap-efficiant.
          (See why in the documentation of refcounted).
      
      These benefits are gained because Interface derives from refcounted
      which has the same benefits. Similar benefits would be gained
      by deriving from IUnknown or any other class that maintains the
      reference counting internally and is adopted to the p<> template
      using the smartptr_xxx () overloaded functions.

      The difference between refcounted and Interface is that Interface is
      more suited for using where multiple inheritance is used. This is due
      to that it derives virtual from refcounted. The virtual derivation
      results in that it can function as base class for any class that is
      intended to be derived from with multiple inheritance, which is a
      typical case when using an interface based design.

      Note that the p<> template does not require it's type to be derived
      from refcounted nor Interface. It's just a help and it gives the
      mentioned benefits.

      The only thing to keep in mind is to never mix classes that derive
      (direct or indirectly) from refcounted and classes that do not derive
      (direct or indirectly) from refcounted in the same class hierarchy.
      Doing so by mistake will be caught by a compilation
      error in the function smartptr_assert_type_compatibility().
    */
    struct Interface : virtual refcounted
    {
    };


    inline void addref (refcounted* o) {
      o->refcount_.increment();
    }
    inline void release (refcounted* o) {
      if (o->refcount_.decrement()) o->destroy();
    }
    inline long getrefcount (const refcounted* o) {
      return o->refcount_.getvalue();
    }



    // Forward declaration of p
    template <typename T> class p;
    template <typename T> class CollItemRef;

    // This is an internal class that helps getting the private member obj_ from p.
    // The normal thing would be to use "friend" but that is not possible since
    // there are template methods and classes that must be friends.
    struct p_help {
      template <typename T> inline static void* get_rc (const p<T>& sp) {
        return const_cast<void*> (sp.rc_);
      }
      template <typename T> inline static void set_rc (p<T>& sp, void* rc) {
        sp.rc_ = rc;
      }
      template <typename T> inline static T* get_obj (const p<T>& sp) {
        return const_cast<T*> (sp.obj_);
      }
      template <typename T> inline static void set_obj (p<T>& sp, T* obj) {
        sp.obj_ = obj;
      }
      template <typename T> inline static T* get_obj (const p<T>& sp, int ptr_adjustment) {
        if (sp.obj_) {
          return const_cast<T*> (
            reinterpret_cast<const T*> (
              reinterpret_cast<const char*>(sp.obj_) + ptr_adjustment));
        } else {
          return NULL;
        }
      }
      template <typename T> inline static void adjust_ptr (p<T>& sp, int ptr_adjustment) {
        if (sp.obj_) *reinterpret_cast<char**>(&sp.obj_) += ptr_adjustment;
      }
      template <typename T> inline static T* get_obj (const CollItemRef<T>& cir) {
        return get_obj (*cir.pitem_, cir.ptrdiff_);
      }
      template <typename T> inline static void* get_rc (const CollItemRef<T>& cir) {
        return cir.pitem_->rc_;
      }
    };

    /** Collection Item Reference.
      Used in collections that must support casting of the collection.
    */
    template <typename T> class CollItemRef {
      friend struct p_help;
    private:
      refcounted* collection_;
      p<T>* pitem_;
      int ptrdiff_;
    public:
      CollItemRef (
          refcounted* collection,
          p<T>* pitem,
          int ptrdiff)
      : collection_ (collection),
          pitem_ (pitem),
          ptrdiff_ (ptrdiff)
      {
        // Add refcount on the collection.
        // This protects pitem_ from the risk of deletion.
        addref (collection);
      }
      ~CollItemRef () {
        // Release refcount on the collection.
        release (collection_);
      }
    public:
      void operator = (const p<T>& obj) {
        *pitem_ = obj;
        p_help::adjust_ptr (*pitem_, 0 - ptrdiff_);
      }
      T* operator -> () const {
        return p_help::get_obj (*pitem_, ptrdiff_);
      }
      T& operator * () const {
        return *p_help::get_obj (*pitem_, ptrdiff_);
      }
    };

    /** Checks that static cast is valid and returns the ptr difference
        between them.
    */
    template <typename T, typename DerivedItem> struct ptr_diff {
      static int value () {
        return
          (int) (reinterpret_cast<const char*> (
            static_cast<T*> (
              reinterpret_cast<DerivedItem*>(1))) -
          reinterpret_cast<const char*> (1));
      }
    };

    /** Generic thread-safe reference counted smart pointer for any type.

        This smart pointer works identically to ordinary pointers in almost
        every way except that it will automatically delete the instance
        as soon as it is not referenced anymore.

        In similarity of ordinary pointers, this smart pointer supports:
            * automatic up-cast
            * explicit down-cast (see p_cast<>)

        Examples of usage:

            // Examples that shows how similar it is to ordinary pointers:

            p<MyClass> myClass = new MyClass();
            p<int> myInt = new int (3);
            p<IMyInterface> myInterface = new MyClassThatDerivesFromIMyInterface();

            myClass->doThis();
            myInterface->doThat();
            ++(*myInt);

            // Casting examples:

            p<MyBaseClass> myBaseClass = mySubClass; // Automatic up-cast.
            mySubClass = p_cast<MySubClass> (myBaseClass); // Static down-cast.
    */
    template <typename T> class p {
      friend struct p_help;
    private:
      void* rc_;
      T* obj_;
    public:
      /** Default constructor. Initiates pointer and refcounter object to zero.
      */
      inline p () : rc_(NULL), obj_ (NULL) {}
      /** Constructor taking ownership of given pointer. Caller looses the ownership of given object.
      */
      inline p (T* obj) {
        if (obj) {
          rc_ = smartptr_assign (
            reinterpret_cast<T*>(obj),
            reinterpret_cast<T*>(NULL));
          obj_ = obj;
        } else {
          rc_ = NULL;
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
      inline ~p () {
        if (rc_) smartptr_release (rc_, reinterpret_cast<T*>(NULL));
      }
      /** Copy Constructor. Increments reference count since we also own the object now.
      */
      inline p (const p& other) : rc_ (other.rc_), obj_ (other.obj_) {
        if (other.rc_) smartptr_addref (
          other.rc_,
          reinterpret_cast<T*>(NULL));
      }
      /** Copy Constructor for p of compatible class.
        Increments reference count since we also own the object now.
      */
      template <class DerivedClass> p (const p<DerivedClass>& other) {
        // If compilation fails here, your assigning a pointer from
        // a pointer to a class (DerivedClass) that is not a child of T.
        obj_ = p_help::get_obj(other);
        rc_ = p_help::get_rc(other);
        // Check compile-time that T and DerivedClass are compatible
        smartptr_assert_type_compatibility ((T*)NULL, (T*)NULL, (DerivedClass*)NULL);
        // Increment refcounter
        if (rc_) smartptr_addref (rc_, reinterpret_cast<T*>(NULL));
      }
      /** Construct from a Collection Item Reference.
      */
      template <class DerivedClass> p (const CollItemRef<DerivedClass>& item)
      {
        // If compilation fails here, your assigning a pointer from
        // a pointer to a class (DerivedClass) that is not a child of T.
        obj_ = p_help::get_obj(item);
        rc_ = p_help::get_rc (item);
        // Check compile-time that T and DerivedClass are compatible
        smartptr_assert_type_compatibility ((T*)NULL, (T*)NULL, (DerivedClass*)NULL);
        // Increment refcounter
        if (rc_) smartptr_addref (rc_, reinterpret_cast<T*>(NULL));
      }
      /** Assignment operator. Increments reference count since we also own the object now.
          Decrements refcount of the old object because we don't own that anymore.
      */
      inline p& operator = (const p& other) {
        if (other.rc_) smartptr_addref (other.rc_, reinterpret_cast<T*>(NULL));
        if (rc_) smartptr_release (rc_, reinterpret_cast<T*>(NULL));
        rc_ = other.rc_;
        obj_ = other.obj_;
        return *this;
      }
      /** Assignment operator for p of compatible class.
        Increments reference count since we also own the object now. Decrements refcount
        of the old object because we don't own that anymore.
      */
      template <class DerivedClass> p& operator = (const p<DerivedClass>& other) {
        void* other_rc = p_help::get_rc(other);
        if (other_rc) smartptr_addref (
            other_rc,
            reinterpret_cast<DerivedClass*>(NULL));
        if (rc_) smartptr_release (
            rc_,
            reinterpret_cast<T*>(NULL));
        // Copy reference counter.
        rc_ = other_rc;
        // Check compile time that DerivedClass is derived from T
        obj_ = p_help::get_obj(other);
        // Check compile-time that T and DerivedClass are compatible
        smartptr_assert_type_compatibility ((T*)NULL, (T*)NULL, (DerivedClass*)NULL);
        return *this;
      }
      /** Assign from a Collection Item Reference.
      */
      template <class DerivedClass> p& operator = (const CollItemRef<DerivedClass>& other) {
        void* other_rc = p_help::get_rc(other);
        if (other_rc) smartptr_addref (
            other_rc,
            reinterpret_cast<DerivedClass*>(NULL));
        if (rc_) smartptr_release (
            rc_,
            reinterpret_cast<T*>(NULL));
        // Copy reference counter.
        rc_ = other_rc;
        // Check compile time that DerivedClass is derived from T
        obj_ = p_help::get_obj(other);
        // Check compile-time that T and DerivedClass are compatible
        smartptr_assert_type_compatibility ((T*)NULL, (T*)NULL, (DerivedClass*)NULL);
        return *this;
      }
      /** Assignment operator taking ownership of given pointer.
        Caller looses the ownership of given object. Decrements refcount
        of the old object because we don't own that anymore.
      */
      p& operator = (T* obj) {
        if (rc_) smartptr_release (rc_, reinterpret_cast<T*>(NULL));
        if (obj) {
          rc_ = smartptr_assign (obj, reinterpret_cast<T*>(NULL));
          obj_ = obj;
        } else {
          rc_ = NULL;
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
      inline T* operator -> () const {
        return obj_;
      }
      /** Reference reach operator. 
      */
      inline T& operator * () const {
        return *obj_;
      }
      /** Equal Operator. Compares the container's pointer value. Used in order to insert the p in std::maps and std::set.
      */
      inline bool operator == (const p& other) const {
        return rc_ == other.rc_;
      }
      /** Equal Operator. Compares the pointer value. Used only to compare with NULL.
      */
      inline bool operator == (const T* other) const {
        return obj_ == other;
      }
      /** No-Equal Operator. Compares the container's pointer value. Used in order to insert the p in std collections.
      */
      bool operator != (const p& other) const {
        return rc_ != other.rc_;
      }
      /** No-Equal Operator. Compares the pointer value. Used mainly to compare with NULL.
      */
      bool operator != (const T* other) const {
        return obj_ != other;
      }
      /** Less Operator. Compares the container's pointer value. Used in order to insert the p in std collections.
      */
      bool operator < (const p& other) const {
        return rc_ < other.rc_;
      }
    };

    /**
    */
    template <typename T> T* getptr (const p<T>& sp) {
      return p_help::get_obj(sp);
    }

    /**
    */
    template <typename T> p<T> smartify (T* ptr) {
      p<T> retval;
      if (ptr) {
        p_help::set_rc (retval, smartptr_smartify (ptr, (T*) NULL));
        p_help::set_obj (retval, ptr);
      }
      return retval;
    }
    template <typename T> p<const T> smartify (const T* ptr) {
      p<const T> retval;
      p_help::set_rc (retval, smartptr_smartify (ptr, (const T*) NULL));
      p_help::set_obj (retval, ptr);
      return retval;
    }
    template <typename T> T* newptr (T* ptr) {
      return ptr;
    }

    /* Used to disable warnings waying "statement has no effect" (gcc warning) in casts.
    */
    /*template <typename T> inline T* warning_workaround_get_ptr(T* inout) {
        return inout;
    }*/

    /** p_cast. static_cast for smart pointers.
      Usage:
        p<Base> b;
        p<Derived> d = p_cast<Derived> (b);

      See also p_dyncast<>
    */
    template <typename T, typename Base>
    inline p<T> p_cast (const p<Base>& pb)
    {
      // Check compile-time that T and Base are compatible
      smartptr_assert_type_compatibility (
          (Base*)NULL,
          (Base*)NULL,
          (T*)NULL); //static_cast<const T*> ((const Base*)(NULL)));
      p<T> retval;
      p_help::set_rc (retval, p_help::get_rc (pb));
      // Checks that the cast is valid as a static_cast:
      p_help::set_obj (retval, static_cast<T*> (p_help::get_obj (pb)));
      if (retval != NULL) addref (retval);
      return retval;
    }
    template <typename T, typename Base>
    inline p<T> p_cast (const CollItemRef<Base>& pb)
    {
      // Check compile-time that T and Base are compatible
      smartptr_assert_type_compatibility (
          (Base*)NULL,
          (Base*)NULL,
          (T*)NULL); //static_cast<const T*> ((const Base*)(NULL)));
      p<T> retval;
      p_help::set_rc (retval, p_help::get_rc (pb));
      // Checks that the cast is valid as a static_cast:
      p_help::set_obj (retval, static_cast<T*> (p_help::get_obj (pb)));
      if (retval != NULL) addref (retval);
      return retval;
    }

    /** p_dyncast. dynamic_cast<> for smart pointers.
      Usage:
        p<Base> b;
        p<Derived> d = p_dyncast<Derived> (b);
    */
    template <typename T, typename Base>
    inline p<T> p_dyncast (const p<Base>& pb) throw (std::bad_cast)
    {
      // Check compile-time that T and Base are compatible
      smartptr_assert_type_compatibility (
          (Base*)NULL,
          (Base*)NULL,
          (T*)NULL); //static_cast<const T*> ((const Base*)(NULL)));
      p<T> retval;
      // Checks that the cast is valid as a dynamic_cast:
      p_help::set_obj (retval, dynamic_cast<T*> (p_help::get_obj (pb)));
      p_help::set_rc (retval, p_help::get_rc (pb));
      if (retval != NULL) addref (retval);
      return retval;
    }
    template <typename T, typename Base>
    inline p<T> p_dyncast (const CollItemRef<Base>& pb) throw (std::bad_cast)
    {
      // Check compile-time that T and Base are compatible
      smartptr_assert_type_compatibility (
          (Base*)NULL,
          (Base*)NULL,
          (T*)NULL); //static_cast<const T*> ((const Base*)(NULL)));
      p<T> retval;
      // Checks that the cast is valid as a dynamic_cast:
      p_help::set_obj (retval, dynamic_cast<T*> (p_help::get_obj (pb)));
      p_help::set_rc (retval, p_help::get_rc (pb));
      if (retval != NULL) addref (retval);
      return retval;
    }

    /**
    */
    template <typename T> struct void_refcounter : refcounted {
      T* obj_;
      void_refcounter (T* obj, long initcount) : refcounted (initcount), obj_ (obj) {}
      virtual ~void_refcounter () {
        delete obj_;
      }
    };
    /* Overloaded reference counting functions for p<T>
    */
    template <typename T> inline void addref (const p<T>& sp) {
      smartptr_addref (
        p_help::get_rc(sp),
        reinterpret_cast<T*>(NULL));
    }
    template <typename T> inline void release (const p<T>& sp) {
      smartptr_release (
        p_help::get_rc(sp),
        reinterpret_cast<T*>(NULL));
    }
    template <typename T> inline long getrefcount (const p<T>& sp) {
      return smartptr_getrefcount (
        p_help::get_rc(sp),
        reinterpret_cast<T*>(NULL));
    }


    /* Overloaded smartptr functions for void
    */
    template <typename T> inline void* smartptr_assign (T* obj, const void*) {
      return static_cast<refcounted*> (new void_refcounter<T> (obj, 1));
    }
    template <typename T> inline void smartptr_addref (T* rc, const void*) {
      addref (const_cast<refcounted*> (reinterpret_cast<const refcounted*> (rc)));
    }
    template <typename T> inline void smartptr_release (T* rc, const void*) {
      release (const_cast<refcounted*> (reinterpret_cast<const refcounted*> (rc)));
    }
    // void* is compatible with void*
    template <typename T> inline void smartptr_assert_type_compatibility (
        const T*, const void*, const void*)
    {
    }
    template <typename T> long smartptr_getrefcount (const T* rc, const void*) {
      return getrefcount (reinterpret_cast<const refcounted*>(rc));
    }
    template <typename T> void* smartptr_smartify (const T* obj, const void*) {
      // This constructor is not ment to compile!
      // It is here to generate an error when trying to call smartify() on
      // an ordinary pointer that is not possible to convert to a smart pointer.
      // This error occurs when the class does not derive from Interface,
      // refcounted, IUnknown or any other class that supports it's own
      // reference counting mechanism.
      ERROR_TYPE_NOT_POSSIBLE_TO_SMARTIFY(obj);
      return NULL;
    }


    /* Overloaded smartptr functions for refcounted
    */
    template <typename T> void* smartptr_assign (T* obj, const refcounted*) {
//#ifdef NDEBUG
      // Release Version. refcounted's default constructor is set to 1
      // (as an optimization) so we don't need to call addref() on it.
      return static_cast<refcounted*> (obj);
/*#else
      // Debug version. refcounted's default constructor is set to 0 just
      // so that we can assert here that p<T>::operator = (T*) is used
      // to assign a newly created instance. If your asserting complains here,
      // it is because the instance you are trying to assign from has already
      // been used in another smart pointer. You can do what you want though
      // by using smartify(p) instead of p as the argument to p<>'s
      // constructor or assigment operator.

      //assert (getrefcount(obj) == 0); // Direct use of T* instead of
                                       // smartify(T*). See explanation
                                       // above.
      addref (obj);
      return static_cast<refcounted*> (obj);
#endif*/
    }
    template <typename T> void* smartptr_smartify (const T* obj, const refcounted*) {
      addref (const_cast<refcounted*> (static_cast<const refcounted*> (obj)));
      return const_cast<refcounted*> (static_cast<const refcounted*> (obj));
    }
  }
}

/** IUnknown - Makes p<> hold COM objects according to it's standard.
*/
struct IUnknown;

namespace ifr {
  namespace v1 {
    /* Overloaded smartptr functions for IUnknown
    */
    template <typename T> void* smartptr_assign (T* obj, const IUnknown*) {
      // Assert this is a newly created instance. If your asserting complains here,
      // you are probably not assigning a newly created instance but an already
      // existing instance. If so, you should add the function smartify() around
      // the instance that you are assigning back as a smart pointer.
      assert (getrefcount(obj) == 1);
      return static_cast<IUnknown*> (obj);
    }
    template <typename T> inline void smartptr_addref_IUnknown (T* obj) {
      obj->AddRef();
    };
    template <typename T> inline void smartptr_addref (T* rc, const IUnknown*) {
      smartptr_addref_IUnknown (const_cast<IUnknown*> (reinterpret_cast<const IUnknown*> (rc)));
    }
    template <typename T> inline void smartptr_release_IUnknown (T* obj) {
      return obj->Release();
    };
    template <typename T> inline void smartptr_release (T* rc, const IUnknown*) {
      smartptr_release_IUnknown (const_cast<IUnknown*> (reinterpret_cast<const IUnknown*> (rc)));
    }
    // IUnknown* is compatible with IUnknown*
    template <typename T> inline void smartptr_assert_type_compatibility (const T*, const IUnknown*, const IUnknown*) {}

    // IUnknown* is not compatible with void*
    template <typename T> inline void smartptr_assert_type_compatibility (const T* tmp, const void*, const IUnknown*) {
      // This overloaded method must not compile.
      // It is here to generate an error when trying to mix instances of
      // IUnknown and non-IUnknown. If you get a compilation error here,
      // you should derive from IUnknown already in given base
      // class and not only in given subclass. If that is not possible, your
      // subclass must not derive from IUnknown.
      ERROR_CANNOT_MIX_IUNKNOWN_WITH_NON_IUNKNOWN(tmp);
    }
    template <typename T> inline void smartptr_assert_type_compatibility (const T*, const IUnknown* a, const void* b) {
      smartptr_assert_type_compatibility (b, b, a);
    }
    template <typename T> long smartptr_getrefcount_IUnknown (T* obj) {
      obj->AddRef();
      return obj->Release();
    }
    template <typename T> long smartptr_getrefcount (const T* rc, const IUnknown*) {
      return smartptr_getrefcount_IUnknown (const_cast<IUnknown*> (reinterpret_cast<const IUnknown*>(rc)));
    }
    template <typename T> void* smartptr_smartify (const T* obj, const IUnknown*) {
      smartptr_addref_IUnknown (const_cast<IUnknown*> (static_cast<const IUnknown*> (obj)));
      return const_cast<IUnknown*> (static_cast<const IUnknown*> (obj));
    }
  }
}


#endif // IFR_V1_P_HPP_

