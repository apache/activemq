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
#ifndef IFR_V1_ARRAY_HPP_
#define IFR_V1_ARRAY_HPP_

#include <cassert> // assert
#include <cstdlib> // size_t, malloc
#include <cstring> // memset, memcpy, memcmp
#include <new>     // bad_alloc
#include <memory>  // auto_ptr
#include "p.hpp"
#include "colltraits.hpp"

/*
*/
namespace ifr {
  namespace v1 {
    using std::size_t;
    using std::malloc;
    using std::memset;
    using std::bad_alloc;
    using std::memcpy;
    using std::memcmp;
    using std::auto_ptr;

    /* Base of refcounted array types.
       Defines it's own new and delete operators that
       allocated memory on heap both for the class itself
       and for the number of items in the array.
    */
    template <class C, typename ItemType> class RCArrayBase : public refcounted {
    protected:
      size_t size_;
    private:
      // Hide the normal new and delete operators.
      void* operator new (size_t);
      void* operator new[] (size_t);
      void operator delete[] (void*);
    public:
      // Allocate space for both this class and the array that it holds.
      void* operator new (size_t object_size, size_t array_size) throw (bad_alloc) {
        assert (object_size == sizeof(C));
        (void) object_size; // Remove warning about unreferenced variable

        C* p = reinterpret_cast<C*> (
          malloc (sizeof(C) + (array_size * sizeof(ItemType))));
        if (p == NULL) {
          throw bad_alloc ();
        }
        // This is kind of a pre-constructor that sets the size value since
        // it is not accessable in constructor.
        p->size_ = array_size;
        return p;
      }
      void operator delete (void* p, size_t) {
        free (p);
      }
      void operator delete (void* p) {
        operator delete (p, 0);
      }
      virtual void destroy() {
        delete static_cast<C*> (this);
      }
    protected:
      // Make impossible for end-users to create an instance of us except through create() or
      // clone() methods on our subclasses.
      RCArrayBase(){}
    private:
      // Make non-copyable since we have a dynamic operator new.
      // Instead, our subclasses can be cloneable through their clone() method.
      RCArrayBase(const RCArrayBase& other);
      RCArrayBase& operator = (const RCArrayBase& other);
    public:
      ItemType* c_array() {
        assert (this != NULL);
        return reinterpret_cast<ItemType*> (reinterpret_cast<char*> (this) + sizeof(C));
      }
      const ItemType* c_array() const {
        assert (this != NULL);
        return reinterpret_cast<const ItemType*> (reinterpret_cast<const char*> (this) + sizeof(C));
      }
      ItemType& at (size_t pos) {
        assert (this != NULL);
        return c_array()[pos];
      }
      const ItemType& at (size_t pos) const {
        assert (this != NULL);
        return c_array()[pos];
      }
      size_t size() const {
        assert (this != NULL);
        return size_;
      }
    };

    /* Reference counted array template.
       Used by the Array and array class templates.
       This general template class is for arrays items of non-primitive types.
       A specialization exists for array items of primitive types.
    */
    template <
      typename T,
      typename ItemType = typename colltraits<T>::ItemType,
      bool     value_based = is_primitive<T>::value >
    class RCArray
      : public RCArrayBase<RCArray<T, ItemType, value_based>, ItemType>
    {
    public:
      RCArray() {
        // Zero fill (instead of calling default constuctor of all items (p<T>))
        // size_ member was initialized by new operator.
        memset (
          this->c_array(),
          0,
          this->size_ * sizeof(ItemType));
      }
      ~RCArray() {
        if (this->size_) {
          // Call destructor of the smart pointers
          for (size_t i=0; i<this->size_; ++i) {
            this->c_array()[i].ItemType::~ItemType();
          }
        }
      }
      static RCArray* create (size_t size) {
        return new (size) RCArray();
      }
      RCArray* clone() const {
        assert (this != NULL);
        auto_ptr<RCArray> retval (new (this->size_) RCArray ());
        ItemType* retarray = retval->c_array();
        const ItemType* thisarray = this->c_array();
        // Copy the smart pointers only
        for (size_t i=0; i<this->size_; ++i) {
          retarray[i] = thisarray[i];
        }
        return retval.release();
      }

      int compare (const RCArray& other) const {
        // Compare the smart pointers only
        for (size_t i=0; i < this->size(); ++i) {
          if (this->at(i) == other.at(i)) {
            continue;
          } else if (this->at(i) < other.at(1)) {
            return -1;
          } else {
            return 1;
          }
        }
        return 0;
      }
    };

    /**
    */
    enum SecureMode {
      NOT_SECURE,
      SECURE
    };

    /* Reference counted array template specialization for primitive types.
       Used by the Array and array class templates.
    */
    template <typename T, typename ItemType>
    class RCArray<T, ItemType, true>
      : public RCArrayBase<RCArray<T, ItemType, true>, ItemType>
    {
    private:
      SecureMode secmode_;
    public:
      RCArray(SecureMode secmode = NOT_SECURE) : secmode_(secmode) {}
      static RCArray* create (size_t size) {
        return new (size) RCArray();
      }
      static RCArray* create (size_t size, SecureMode secmode) {
        return new (size) RCArray(secmode);
      }
      ~RCArray() {
        if (secmode_ && this->size_) {
          // Zero memory
          const void* p = this->c_array();
          memset (
            const_cast<void*>(p),
            0,
            this->size_ * sizeof(T));
          // Force no compiler optimization:
          *(volatile char*)p = *(volatile char*)p;
        }
      }
      RCArray* clone() const {
        assert (this != NULL);
        auto_ptr<RCArray> retval (new (this->size_) RCArray (secmode_));
        memcpy (
          const_cast<ItemType*>(retval->c_array()),
          this->c_array(),
          this->size_ * sizeof(ItemType));
        return retval.release();
      }
      bool isSecure() const {
        assert (this != NULL);
        return (secmode_ == SECURE);
      }
      int compare (const RCArray& other) const {
        for (size_t i=0; i < this->size(); ++i) {
          if (this->at(i) == other.at(i)) {
            continue;
          } else if (this->at(i) < other.at(1)) {
            return -1;
          } else {
            return 1;
          }
        }
        return 0;
      }
    };

    /* Forward declaration of the array template.
       Needed for the clone() methods.
    */
    template <
      typename T,
      typename ItemType = typename colltraits<T>::ItemType,
      bool is_primitive = is_primitive<T>::value>
    class array;

    /* Specialization of colltraits for array.
      Needed for collections where the item type is array,
      for example array<array<T> >.
      This specialization states that collections of array<> will
      store each item as a array<> and not as a p< array<> >.
    */
    template <typename T, typename Item_Type, bool is_primitive>
    struct colltraits<array<T, Item_Type, is_primitive>, false>
    {
      typedef array<T, Item_Type, is_primitive> ItemType;
      typedef const array<T, ItemType, is_primitive>& InArgType;
    };

    /* Friend class helping to access the private a_ member in array.
      I want the a_ member to be private so that no one uses it by
      accident. Still, when people know what the're doing, it should
      be possible to access the member using this help class.
    */
    struct array_help {
      template <typename T, typename ItemType, bool is_primitive>
      inline static const p<RCArray<T,ItemType,is_primitive> >& get_array (
        const array<T, ItemType, is_primitive>& arr)
      {
        return arr.a_;
      }

      template <typename T, typename ItemType, bool is_primitive>
      inline static p<RCArray<T,ItemType,is_primitive> >& get_array (
        array<T, ItemType, is_primitive>& arr)
      {
        return arr.a_;
      }

      template <typename T, typename ItemType>
      inline static int get_ptrdiff (const array<T, ItemType, false>& arr) {
        return arr.ptrdiff_;
      }

      template <typename T, typename ItemType>
      inline static void set_ptrdiff (
          array<T, ItemType, false>& arr,
          int ptrdiff)
      {
        arr.ptrdiff_ = ptrdiff;        
      }
    };

    /* Base help class for the array template. 
       The class is here to define the common interface between
       array<primitive type> and array<non primitive type>.
    */
    template <typename T, typename ItemType, bool is_primitive>
    class array_base
    {
      friend struct array_help;
    public:
      typedef RCArray<T,ItemType,is_primitive> ArrayClass;
    protected:
      // The only member - a smart pointer to an array instance.
      p<ArrayClass> a_;
    public:
      // Constructors
      inline array_base (){}
      inline array_base (const p<ArrayClass>& other) : a_ (other) {}
      // Methods
      inline size_t size() const {
        return a_->size();
      }
      inline int compare (const array_base& other) const {
        return a_->compare (*other.a_);
      }
      // Operators 
      inline bool operator == (const ArrayClass* arrayClass) const {
        return a_ == arrayClass;
      }
      inline bool operator != (const ArrayClass* arrayClass) const {
        return a_ != arrayClass;
      }
      inline bool operator < (const array_base& other) const {
        return a_->compare (*other.a_) < 0;
      }
      inline bool operator > (const array_base& other) const {
        return a_->compare (*other.a_) > 0;
      }
      inline bool operator == (const array_base& other) const {
        return a_->compare (*other.a_) == 0;
      }
      inline bool operator != (const array_base& other) const {
        return a_->compare (*other.a_) != 0;
      }
    };

    /* The array template.
       This is the generic array class where all items are p<T>.
       Note that a specialization exists for primitive types.
    */
    template <typename T, typename ItemType, bool is_primitive>
    class array
      : public array_base<T, ItemType, is_primitive>
    {
      friend struct array_help;
    public:
      typedef RCArray<T, ItemType, is_primitive> ArrayClass;
      typedef array_base<T, ItemType, is_primitive> Base;
    private:
      int ptrdiff_;
    public:
      // Constructors
      inline array () : ptrdiff_(0) {}
      inline explicit array (size_t size) : Base ((size ? ArrayClass::create(size) : NULL)), ptrdiff_(0) {}
      inline array (const NullClass*) : Base (NULL), ptrdiff_(0) {}
      //inline array (const p<ArrayClass>& other) : Base (other), ptrdiff_(0) {}
      inline array (const array& other) : Base (other), ptrdiff_(other.ptrdiff_) {}
      // Template based Copy Constructor
      template <typename DerivedItem>
        inline array (const array<DerivedItem, p<DerivedItem>, false>& other) :
          Base (reinterpret_cast<const Base&> (other))
      {
        // Check compile time that DerivedItem is derived from T
        T* tmp = reinterpret_cast<DerivedItem*>(NULL);
        // Check compile-time that T and DerivedItem are compatible
        smartptr_assert_type_compatibility (tmp, tmp, (DerivedItem*)NULL);
        // Set ptrdiff_ according to the cast made
        ptrdiff_ = reinterpret_cast<const array&>(other).ptrdiff_ + ptr_diff<T, DerivedItem>::value();
      }
      inline array (const p<ArrayClass>& other, int ptrdiff) : Base (other), ptrdiff_(ptrdiff) {}
    public:
      // Template based operator =
      template <typename DerivedItem>
        inline array& operator = (const array<DerivedItem, p<DerivedItem>, false>& other)
      {
        // Check compile time that DerivedItem is derived from T
        T* tmp = reinterpret_cast<DerivedItem*>(NULL);
        // Check compile-time that T and DerivedItem are compatible
        smartptr_assert_type_compatibility (tmp, tmp, (DerivedItem*)NULL);
        // Set ptrdiff_ according to the cast made
        ptrdiff_ = reinterpret_cast<const array&>(other).ptrdiff_ + ptr_diff<T, DerivedItem>::value();
        this->a_ = reinterpret_cast<const array&>(other).a_;
        return *this;
      }
      inline array& operator = (const NullClass* nullclass) {
        assert (nullclass == NULL);
        this->a_ = NULL;
        return *this;
      }
      inline CollItemRef<T> operator [] (size_t pos) const {
        return CollItemRef<T> (getptr(this->a_), this->a_->c_array() + pos, ptrdiff_);
      }
      // Methods
      inline CollItemRef<T> at (size_t pos) const {
        return CollItemRef<T> (getptr(this->a_), this->a_->c_array() + pos, ptrdiff_);
      }
      inline array clone() const {
        return array (this->a_->clone(), ptrdiff_);
      }
    };

    /* The array template specialized for primitive types.
    */
    template <typename T, typename ItemType>
    class array<T,ItemType,true>
      : public array_base<T,ItemType,true>
    {
    public:
      typedef RCArray<T,ItemType,true> ArrayClass;
      typedef array_base<T,ItemType,true> Base;
    public:
      // Ordinary Constructors
      inline array () {}
      inline explicit array (size_t size) : Base ((size ? ArrayClass::create (size) : NULL)) {}
      inline array (size_t size, SecureMode secmode) : Base (ArrayClass::create(size, secmode)) {}
      inline array (const NullClass*) : Base (NULL) {}
      inline array (const p<ArrayClass>& other) : Base (other) {}
      inline array (const array& other) : Base (other) {}
      // Constructor that copies content from supplied C array.
      array (const ItemType* initialData, size_t size, SecureMode secmode = NOT_SECURE) :
        Base ((size ? ArrayClass::create (size, secmode) : NULL))
      {
        if (size) {
          memcpy (const_cast<typename remove_const<ItemType>::Type*> (c_array()), initialData, size * sizeof(ItemType));
        }
      }
      inline array& operator = (const NullClass* nullclass) {
        assert (nullclass == NULL);
        this->a_ = NULL;
        return *this;
      }
      inline ItemType& operator [] (size_t pos) {
        return this->a_->at(pos);
      }
      inline const ItemType& operator [] (size_t pos) const {
        return this->a_->at(pos);
      }
      // Methods
      inline ItemType& at (size_t pos) {
        return this->a_->at (pos);
      }
      inline const ItemType& at (size_t pos) const {
        return this->a_->at (pos);
      }
      inline array clone() const {
        return this->a_->clone();
      }
      inline ItemType* c_array() {
        return this->a_->c_array();
      }
      inline const ItemType* c_array() const {
        return this->a_->c_array();
      }
    };

    /**
    */
    template <typename T, typename ItemType, bool is_primitive>
    long getrefcount (const array<T, ItemType, is_primitive>& parray) {
      return getrefcount (array_help::get_array (parray));
    }

    /**
    */
    template <typename T, typename ItemType, bool is_primitive>
    void addref (array<T, ItemType, is_primitive>& parray) {
      addref (array_help::get_array (parray));
    }

    /**
    */
    template <typename T, typename ItemType, bool is_primitive>
    void release (array<T, ItemType, is_primitive>& parray) {
      release (array_help::get_array (parray));
    }

    /** array_cast. Similar to static_cast but for arrays.
      Usage:
        array<Base> b;
        array<Derived> d = array_cast<Derived> (b);
    */
    template <typename T, typename Base>
    inline array<T, p<T>, false> array_cast (const array<Base, p<Base>, false>& baseArray)
    {
      // Check compile-time that T and Base are compatible
      // Also check that the cast is valid using static_cast but allowing casting between const/non-const.
      smartptr_assert_type_compatibility (
          (Base*)NULL,
          (Base*)NULL,
          static_cast<const T*> ((Base*)(0))) // <-- Checks that cast is valid using static_cast + const_cast
          ;
      // Cast and adjust ptrdiff in returned array (making our own static_cast<>-similar implementaion)
      return array<T> (
        reinterpret_cast<const p<RCArray<T> >&> (array_help::get_array (baseArray)),
        array_help::get_ptrdiff (baseArray) - ptr_diff<Base, T>::value());
    }
    /** array_cast for arrays of primitive types.
      Usage:
        array<char> a;
        array<const unsigned char> b = array_cast<const unsigned char> (a);
    */
    template <typename T, typename T2>
    inline array<T, T, true>& array_cast (const array<T2, T2, true>& otherArray)
    {
      ERROR_CANNOT_CAST_PRIMITIVE_ARRAYS(otherArray); // Not implemented.
    }

    /**
    */
    template <typename T> const T* getptr (const array<T>& a) {
      return (a != NULL ? a.c_array() : NULL);
    }
    /**
    */
    template <typename T> T* getptr (array<T>& a) {
      return (a != NULL ? a.c_array() : NULL);
    }

  }
}

#endif // IFR_V1_ARRAY_HPP_
