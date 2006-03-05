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
using System.Collections;

namespace JMS
{
	
	/// <summary>
	/// Represents a Map of primitive types where the keys are all string instances
	/// and the values are strings or numbers.
	/// </summary>
	public interface IPrimitiveMap
    {
        
        void Clear();
        
        bool Contains(object key);
        
        void Remove(object key);
        
        int Count
        {
            get;
        }
        
        ICollection Keys
        {
            get;
        }
        
        ICollection Values
        {
            get;
        }
        
        object this[string key]
        {
            get;
            set;
        }
        
        string GetString(string key);
        void SetString(string key, string value);
        
        bool GetBool(string key);
        void SetByte(string key, bool value);
        
        byte GetByte(string key);
        void SetByte(string key, byte value);
        
        char GetChar(string key);
        void SetChar(string key, char value);
        
        short GetShort(string key);
        void SetShort(string key, short value);
        
        int GetInt(string key);
        void SetInt(string key, int value);
        
        long GetLong(string key);
        void SetLong(string key, long value);
        
        float GetFloat(string key);
        void SetFloat(string key, float value);
        
        double GetDouble(string key);
        void SetDouble(string key, double value);
        
    }
}

