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
#ifndef IFR_V1_COLLTRAITS_HPP_
#define IFR_V1_COLLTRAITS_HPP_

namespace ifr {
  namespace v1 {

    struct value_true {static const bool value = true;};
    struct value_false {static const bool value = false;};

    // is_primitive<>. Tells whether T is a primitive type or not.
    template <typename T> struct is_primitive : value_false{};
    // Pointers are primitive
    template <typename T> struct is_primitive<T*> : value_true {};
    // Enums are primitive
    //template <enum T> struct is_primitive<T> : colltraits_primitive<T> {};
    // All integral types are primitive. Here all integral types are listed:
    template <> struct is_primitive<char> : value_true {};
    template <> struct is_primitive<signed char> : value_true {};
    template <> struct is_primitive<unsigned char> : value_true {};
    template <> struct is_primitive<signed short> : value_true {};
    template <> struct is_primitive<unsigned short> : value_true {};
    #if !defined (_MSC_VER) || defined (_NATIVE_WCHAR_T_DEFINED)
    template <> struct is_primitive<wchar_t> : value_true{};
    #endif
    template <> struct is_primitive<signed int> : value_true{};
    template <> struct is_primitive<unsigned int> : value_true{};
    template <> struct is_primitive<signed long> : value_true{};
    template <> struct is_primitive<unsigned long> : value_true{};
    template <> struct is_primitive<signed long long> : value_true{};
    template <> struct is_primitive<unsigned long long> : value_true{};
    // float and double is primitive
    template <> struct is_primitive<float> : value_true{};
    template <> struct is_primitive<double> : value_true{};
    // bool is primitive
    template <> struct is_primitive<bool> : value_true{};
    // If const T is primitive, then T is primitive as well
    template <typename T> struct is_primitive<const T> : is_primitive<T> {};
    // If volatile T is primitive, then T is primitive as well
    template <typename T> struct is_primitive<volatile T> : is_primitive<T> {};
    // If const volatile T is primitive, then T is primitive as well
    template <typename T> struct is_primitive<const volatile T> : is_primitive<T> {};

    // collection traits for primitive types where ItemType is T
    template <typename T, bool primitive=is_primitive<T>::value> struct colltraits {
      typedef T ItemType;
      typedef const T& InArgType;
      //static const bool is_primitive = primitive;
    };
    // collection traits for non-primitive types where ItemType is p<T>
    template <typename T> struct colltraits<T, false> {
      typedef p<T> ItemType;
      typedef const p<T>& InArgType;
      //static const bool is_primitive = false;
    };

/*    // Array
    template <typename T, typename ItemType, bool is_primitive> class Array;
    template <typename T, typename ItemType, bool is_primitive>
    struct colltraits<Array<T, ItemType, is_primitive>, false>
    {
      typedef array<T, ItemType, is_primitive> ItemType;
      typedef const Array<T, ItemType, is_primitive>& InArgType;
    };
    // String
    class String;
    struct colltraits<String, false>
    {
      typedef pString ItemType;
      typedef const String& InArgType;
    };
*/

    template <typename T> struct remove_const {
      typedef T Type;
    };
    template <typename T> struct remove_const<const T> {
      typedef T Type;
    };
    /*template <typename Type1, typename Type2> struct assert_same_size
    {
        static const bool value = (sizeof(Type1) == sizeof(Type2));
    };
    template <typename Type1, typename Type2> Type1& same_size_cast (Type2& in)
    {
        ERROR_CAST_TO_DIFFERENT_SIZE(in);
    };
    template <typename Type1, typename Type2>*/
  }
}

#endif // IFR_V1_COLLTRAITS_HPP_
