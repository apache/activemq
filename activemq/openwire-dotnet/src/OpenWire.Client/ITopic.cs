using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Summary description for ITopic.
        /// </summary>
        public interface ITopic : IDestination {

                String TopicName {
                        get; 
                } 
        } 
}
