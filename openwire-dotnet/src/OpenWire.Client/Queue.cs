using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Summary description for Queue.
        /// </summary>
        public interface Queue : Destination {

                String QueueName {
                        get; 
                } 
        } 
}
