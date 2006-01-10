using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// An object capable of sending messages to some destination
        /// </summary>
        public interface IMessageProducer : IDisposable {

                /// <summary>
                /// Sends the message to the default destination for this producer
                /// </summary>
                void Send(IMessage message);

                /// <summary>
                /// Sends the message to the given destination
                /// </summary>
                void Send(IDestination destination, IMessage message); 
        } 
}
