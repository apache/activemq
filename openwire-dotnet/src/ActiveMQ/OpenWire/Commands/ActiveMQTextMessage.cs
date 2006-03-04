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

namespace ActiveMQ.OpenWire.Commands
{
    public class ActiveMQTextMessage : ActiveMQMessage, ITextMessage
    {
        public const byte ID_ActiveMQTextMessage = 28;
        
        private String text;
        
        public ActiveMQTextMessage()
        {
        }
        
        public ActiveMQTextMessage(String text)
        {
            this.Text = text;
        }
        
        // TODO generate Equals method
        // TODO generate GetHashCode method
        // TODO generate ToString method
        
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQTextMessage;
        }
        
        
        // Properties
        
        public string Text
        {
            get {
                if (text == null)
                {
                    // now lets read the content
                    
                    byte[] data = this.Content;
                    if (data != null)
                    {
                        // TODO assume that the text is ASCII
                        char[] chars = new char[data.Length];
                        for (int i = 0; i < chars.Length; i++)
                        {
                            chars[i] = (char) data[i];
                        }
                        text = new String(chars);
                    }
                }
                return text;
            }
            
            set {
                this.text = value;
                byte[] data = null;
                if (text != null)
                {
                    // TODO assume that the text is ASCII
                    data = new byte[text.Length];
                    
                    // now lets write the bytes
                    char[] chars = text.ToCharArray();
                    for (int i = 0; i < chars.Length; i++)
                    {
                        data[i] = (byte) chars[i];
                    }
                }
                this.Content = data;
            }
        }
    }
}
