using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Represents a text based message
        /// </summary>
        public interface ITextMessage : IMessage {

                string Text {
                        get;
                        set; 
                } 
        } 
}
