using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Represents a message either to be sent to a message broker or received from a message broker
        /// </summary>
        public interface IMessage {

                IDestination FromDestination {
                        get;
                } 
        } 
}
