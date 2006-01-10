using System;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Summary description for DestinationFilter.
        /// </summary>
        public abstract class DestinationFilter {
                public const String ANY_DESCENDENT = ">";
                public const String ANY_CHILD = "*";

                public bool matches(ActiveMQMessage message) {
                        return matches(message.Destination); 
                }

                public abstract bool matches(ActiveMQDestination destination); 
        } 
}
