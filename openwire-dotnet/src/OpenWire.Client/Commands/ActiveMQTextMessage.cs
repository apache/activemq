using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
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
