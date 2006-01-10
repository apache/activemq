using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// A consumer of messages
        /// </summary>
        public interface IMessageConsumer : IDisposable {

                /// <summary>
                /// Waits until a message is available and returns it
                /// </summary>
                IMessage Receive();
                
                /// <summary>
                /// If a message is available immediately it is returned otherwise this method returns null
                /// </summary>
                IMessage ReceiveNoWait();
                
        } 
}
