using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Exception thrown when a consumer is used that it already closed
        /// </summary>
        public class ConsumerClosedException : OpenWireException {
                public ConsumerClosedException() : base("The consumer is already closed!") {
                }
        } 
}
