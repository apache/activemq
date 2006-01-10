using System;
using System.Collections;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands {
        public class ActiveMQTextMessage : ActiveMQMessage, ITextMessage {
                public const byte ID_ActiveMQTextMessage = 28;

                private String text;

                public ActiveMQTextMessage() {
                }

                public ActiveMQTextMessage(String text) {
                        this.text = text;
                }

                // TODO generate Equals method
                // TODO generate GetHashCode method
                // TODO generate ToString method


                public override byte GetCommandType() {
                        return ID_ActiveMQTextMessage; 
                }


                // Properties

                public string Text {
                        get {
                                if (text == null) {
                                        // TODO parse from the content
                                }
                                return text; 
                        }
                        set { this.text = value; } 
                } 
        } 
}
