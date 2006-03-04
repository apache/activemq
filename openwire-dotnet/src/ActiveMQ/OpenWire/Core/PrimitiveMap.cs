/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// A default implementation of IPrimitiveMap
    /// </summary>
    public class PrimitiveMap : IPrimitiveMap
    {
        private IDictionary dictionary = new Hashtable();
        
        
        /// <summary>
        /// Unmarshalls the map from the given data or if the data is null just
        /// return an empty map
        /// </summary>
        public static PrimitiveMap Unmarshal(byte[] data)
        {
            PrimitiveMap answer = new PrimitiveMap();
            answer.dictionary = BaseDataStreamMarshaller.UnmarshalPrimitiveMap(data);
            return answer;
        }
        
        public byte[] Marshal()
        {
            return BaseDataStreamMarshaller.MarshalPrimitiveMap(dictionary);
        }
        
        
        public void Clear()
        {
            dictionary.Clear();
        }
        
        public bool Contains(Object key)
        {
            return dictionary.Contains(key);
        }
        
        public void Remove(Object key)
        {
            dictionary.Remove(key);
        }
        
        
        public int Count
        {
            get {
                return dictionary.Count;
            }
        }
        
        public ICollection Keys
        {
            get {
                return dictionary.Keys;
            }
        }
        
        public ICollection Values
        {
            get {
                return dictionary.Values;
            }
        }
        
        public object this[string key]
        {
            get {
                return GetValue(key);
            }
            set {
                CheckValidType(value);
                SetValue(key, value);
            }
        }
        
        public string GetString(string key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(string));
            return (string) value;
        }
        
        public void SetString(string key, string value)
        {
            SetValue(key, value);
        }
        
        public bool GetBool(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(bool));
            return (bool) value;
        }
        
        public void SetByte(String key, bool value)
        {
            SetValue(key, value);
        }
        
        public byte GetByte(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(byte));
            return (byte) value;
        }
        
        public void SetByte(String key, byte value)
        {
            SetValue(key, value);
        }
        
        public char GetChar(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(char));
            return (char) value;
        }
        
        public void SetChar(String key, char value)
        {
            SetValue(key, value);
        }
        
        public short GetShort(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(short));
            return (short) value;
        }
        
        public void SetShort(String key, short value)
        {
            SetValue(key, value);
        }
        
        public int GetInt(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(int));
            return (int) value;
        }
        
        public void SetInt(String key, int value)
        {
            SetValue(key, value);
        }
        
        public long GetLong(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(long));
            return (long) value;
        }
        
        public void SetLong(String key, long value)
        {
            SetValue(key, value);
        }
        
        public float GetFloat(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(float));
            return (float) value;
        }
        
        public void SetFloat(String key, float value)
        {
            SetValue(key, value);
        }
        
        public double GetDouble(String key)
        {
            Object value = GetValue(key);
            CheckValueType(value, typeof(double));
            return (double) value;
        }
        
        public void SetDouble(String key, double value)
        {
            SetValue(key, value);
        }
        
        
        
        
        protected virtual void SetValue(String key, Object value)
        {
            dictionary[key] = value;
        }
        
        
        protected virtual Object GetValue(String key)
        {
            return dictionary[key];
        }
        
        protected virtual void CheckValueType(Object value, Type type)
        {
            if (! type.IsInstanceOfType(value))
            {
                throw new OpenWireException("Expected type: " + type.Name + " but was: " + value);
            }
        }
        
        protected virtual void CheckValidType(Object value)
        {
            if (value != null)
            {
                Type type = value.GetType();
                if (! type.IsPrimitive && !type.IsValueType && !type.IsAssignableFrom(typeof(string)))
                {
                    throw new OpenWireException("Invalid type: " + type.Name + " for value: " + value);
                }
            }
        }
        
    }
}
