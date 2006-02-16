using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Summary description for IQueue.
        /// </summary>
        public interface IQueue : IDestination {

                String QueueName {
                        get; 
                } 
        } 
}
