/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {

        public enum AcknowledgementMode {
                Unknown, AutoAcknowledge, ClientAcknowledge, Transactional 
        }


        /// <summary>
        /// Represents a connection with a message broker
        /// </summary>
        public interface IConnection : IDisposable, IStartable, IStopable {

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                ISession CreateSession();

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                ISession CreateSession(bool transacted, AcknowledgementMode acknowledgementMode);


                // Properties

                bool Transacted {
                        get;
                        set; 
                }

                AcknowledgementMode AcknowledgementMode {
                        get;
                        set; 
                }

				String ClientId
				{
					get;
					set; 
				}
 
				
		}
}
