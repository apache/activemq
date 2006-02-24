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
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    /// <summary>
    /// Represents a connection with a message broker
    /// </summary>
    public class Connection : IConnection
    {
        static private char[] MAGIC = new char[] { 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' };
        
        private ITransport transport;
        private ConnectionInfo info;
        private WireFormatInfo wireFormatInfo = new WireFormatInfo();
        private BrokerInfo brokerInfo; // from broker
        private WireFormatInfo brokerWireFormatInfo; // from broker
        private IList sessions = new ArrayList();
        private bool transacted;
        private bool connected;
        private bool closed;
        private AcknowledgementMode acknowledgementMode;
        private long sessionCounter;
        private IDictionary consumers = new Hashtable();// TODO threadsafe
        
        
        public Connection(ITransport transport, ConnectionInfo info)
        {
            this.transport = transport;
            this.info = info;
            this.transport.Command += new CommandHandler(OnCommand);
            this.transport.Start();
        }

		/// <summary>
		/// Starts message delivery for this connection.
		/// </summary>
		public void Start() 
		{
		}
        
		/// <summary>
		/// Stop message delivery for this connection.
		/// </summary>
		public void Stop() 
		{
		}
        
        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession()
        {
            return CreateSession(transacted, acknowledgementMode);
        }
        
        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession(bool transacted, AcknowledgementMode acknowledgementMode)
        {
            CheckConnected();
            SessionInfo info = CreateSessionInfo(transacted, acknowledgementMode);
            SyncRequest(info);
            Session session = new Session(this, info);
            sessions.Add(session);
            return session;
        }
        
        public void Dispose()
        {
            foreach (Session session in sessions)
            {
                session.Dispose();
            }
            sessions.Clear();
            transport.Dispose();
            closed = true;
        }
        
        // Properties
        
        public ITransport ITransport
        {
            get { return transport; }
            set { this.transport = value; }
        }
        
        public bool Transacted
        {
            get { return transacted; }
            set { this.transacted = value; }
        }
        
        public AcknowledgementMode AcknowledgementMode
        {
            get { return acknowledgementMode; }
            set { this.acknowledgementMode = value; }
        }
        
        public string ClientId
        {
            get { return info.ClientId; }
            set {
                if (connected)
                {
                    throw new OpenWireException("You cannot change the ClientId once the Connection is connected");
                }
                info.ClientId = value;
            }
        }
        
        // Implementation methods
        
        /// <summary>
        /// Performs a synchronous request-response with the broker
        /// </summary>
        public Response SyncRequest(Command command)
        {
            Response response = transport.Request(command);
            if (response is ExceptionResponse)
            {
                ExceptionResponse exceptionResponse = (ExceptionResponse) response;
                // TODO include stack trace
                throw new OpenWireException("Request failed: " + exceptionResponse);
            }
            return response;
        }
        
        
        protected SessionInfo CreateSessionInfo(bool transacted, AcknowledgementMode acknowledgementMode)
        {
            SessionInfo answer = new SessionInfo();
            SessionId sessionId = new SessionId();
            sessionId.ConnectionId = info.ConnectionId.Value;
            lock (this)
            {
                sessionId.Value = ++sessionCounter;
            }
            answer.SessionId = sessionId;
            return answer;
        }
        
        protected void CheckConnected()
        {
            if (closed)
            {
                throw new ConnectionClosedException();
            }
            if (!connected)
            {
                // lets configure the wire format
                wireFormatInfo.Magic = CreateMagicBytes();
                wireFormatInfo.Version = 1;
                transport.Oneway(wireFormatInfo);
                
                // now lets send the connection and see if we get an ack/nak
                SyncRequest(info);
                connected = true;
            }
        }
        
        /// <summary>
        /// Register a new consumer
        /// </summary>
        /// <param name="consumerId">A  ConsumerId</param>
        /// <param name="consumer">A  MessageConsumer</param>
        public void AddConsumer(ConsumerId consumerId, MessageConsumer consumer)
        {
            consumers[consumerId] = consumer;
        }
        
        
        /// <summary>
        /// Remove a consumer
        /// </summary>
        /// <param name="consumerId">A  ConsumerId</param>
        public void RemoveConsumer(ConsumerId consumerId)
        {
            consumers[consumerId] = null;
        }
        
        
        /// <summary>
        /// Handle incoming commands
        /// </summary>
        /// <param name="transport">An ITransport</param>
        /// <param name="command">A  Command</param>
        protected void OnCommand(ITransport transport, Command command)
        {
            if (command is MessageDispatch)
            {
                MessageDispatch dispatch = (MessageDispatch) command;
                ConsumerId consumerId = dispatch.ConsumerId;
                MessageConsumer consumer = (MessageConsumer) consumers[consumerId];
                if (consumer == null)
                {
                    Console.WriteLine("ERROR: No such consumer active: " + consumerId);
                }
                else
                {
                    ActiveMQMessage message = (ActiveMQMessage) dispatch.Message;
                    consumer.Dispatch(message);
                }
            }
            else if (command is WireFormatInfo) {
                this.brokerWireFormatInfo = (WireFormatInfo) command;
            }
            else if (command is BrokerInfo) {
                this.brokerInfo = (BrokerInfo) command;
            }
            else
            {
                Console.WriteLine("ERROR:ÊUnknown command: " + command);
            }
        }
        
        /// <summary>
        /// Method CreateMagicBytes
        /// </summary>
        /// <returns>A  byte[]</retutns>
        private byte[] CreateMagicBytes()
        {
            byte[] answer = new byte[MAGIC.Length];
            for (int i = 0; i < answer.Length; i++)
            {
                answer[i] = (byte) MAGIC[i];
            }
            return answer;
        }
    }
}
