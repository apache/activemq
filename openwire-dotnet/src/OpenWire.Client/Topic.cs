using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {
        /// <summary>
        /// Summary description for Topic.
        /// </summary>
        public interface Topic : Destination {

                String TopicName {
                        get; 
                } 
        } 
}
