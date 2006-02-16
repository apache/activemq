using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Exception thrown when the broker returns an error
        /// </summary>
        public class BrokerException : OpenWireException {
                public BrokerException(BrokerError cause) : base("The operation failed: Type: "
                        + cause.ExceptionClass
                        + " stack: "
                        + cause.StackTrace) {
                } 
        } 
}
