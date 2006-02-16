using System;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Represents an exception on the broker
        /// </summary>
        public class BrokerError : AbstractCommand {
                private string exceptionClass;
                private string stackTrace;

                public string ExceptionClass {
                        get { return exceptionClass; }
                        set { exceptionClass = value; } 
                }

                public string StackTrace {
                        get { return stackTrace; }
                        set { stackTrace = value; } 
                } 
        } 
}
