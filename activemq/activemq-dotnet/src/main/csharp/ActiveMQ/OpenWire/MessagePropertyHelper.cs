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
using System.Collections;

namespace ActiveMQ.OpenWire
{
    public delegate object PropertyGetter(ActiveMQMessage message);
    public delegate void PropertySetter(ActiveMQMessage message, object value);
    
    public class MessagePropertyHelper
    {
        private IDictionary setters = new Hashtable();
        private IDictionary getters = new Hashtable();
        
        public MessagePropertyHelper()
        {
            // TODO find all of the JMS properties via introspection
        }
        
        
        public object GetObjectProperty(ActiveMQMessage message, string name) {
            object getter = getters[name];
            if (getter != null) {
            }
            return message.Properties[name];
        }
        
        public void SetObjectProperty(ActiveMQMessage message, string name, object value) {
            PropertySetter setter = (PropertySetter) setters[name];
            if (setter != null) {
                setter(message, value);
            }
            else {
                message.Properties[name] = value;
            }
        }
    }
}
