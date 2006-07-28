/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using ActiveMQ.Commands;
using System;
using ActiveMQ.OpenWire;

namespace ActiveMQ.Commands
{
    
    /// <summary>
    /// Base class for all DataStructure implementations
    /// </summary>
    public abstract class BaseDataStructure : DataStructure
    {
        private bool responseRequired;
        
        public virtual byte GetDataStructureType()
        {
            return 0;
        }
        
        public bool ResponseRequired
        {
            get { return responseRequired; }
            set { this.responseRequired = value; }
        }
        
        public virtual bool IsMarshallAware()
        {
            return false;
        }
        
        
        public virtual void BeforeMarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void AfterMarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void BeforeUnmarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void AfterUnmarshall(OpenWireFormat wireFormat)
        {
        }
        
        public virtual void SetMarshalledForm(OpenWireFormat wireFormat, byte[] data)
        {
        }
        
        public virtual byte[] GetMarshalledForm(OpenWireFormat wireFormat)
        {
            return null;
        }
        
        // Helper methods
        public int HashCode(object value)
        {
            if (value != null)
            {
                return value.GetHashCode();
            }
            else
            {
                return -1;
            }
        }
    }
}
