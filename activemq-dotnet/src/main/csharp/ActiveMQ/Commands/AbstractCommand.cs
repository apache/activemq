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
using ActiveMQ.Commands;
using System;
using ActiveMQ.OpenWire;

namespace ActiveMQ.Commands
{
    
    /// <summary>
    /// Summary description for AbstractCommand.
    /// </summary>
    public abstract class AbstractCommand
    {
        private bool responseRequired;
        
        
        protected AbstractCommand()
        {
        }
        
        public virtual byte GetDataStructureType()
        {
            return 0;
        }
        
        public virtual bool IsMarshallAware()
        {
            return false;
        }
        
        
        
        // Properties
        
        public bool ResponseRequired
        {
            get { return responseRequired; }
            set { this.responseRequired = value; }
        }
        

        
    }
}

