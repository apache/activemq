using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using System.Threading;

namespace OpenWire.Client
{
    /// <summary>
    /// Represents a connection with a message broker
    /// </summary>
    public class Connection : IConnection
    {
        private ITransport transport;
        private ConnectionInfo info;
        private AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
        private BrokerInfo brokerInfo; // from broker
        private WireFormatInfo brokerWireFormatInfo; // from broker
        private IList sessions = new ArrayList();
        private IDictionary consumers = new Hashtable(); // TODO threadsafe
        private bool connected;
        private bool closed;
        private long sessionCounter;
        private long temporaryDestinationCounter;
        private long localTransactionCounter;
        
        
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
            return CreateSession(acknowledgementMode);
        }
        
        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession(AcknowledgementMode acknowledgementMode)
        {
            SessionInfo info = CreateSessionInfo(acknowledgementMode);
            SyncRequest(info);
            Session session = new Session(this, info, acknowledgementMode);
            sessions.Add(session);
            return session;
        }
        
        public void Dispose()
        {
            /*
            foreach (Session session in sessions)
            {
                session.Dispose();
            }
            */
            DisposeOf(ConnectionId);
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
        
        public ConnectionId ConnectionId
        {
            get {
                return info.ConnectionId;
            }
        }
        
        public BrokerInfo BrokerInfo
        {
            get {
                return brokerInfo;
            }
        }
        
        public WireFormatInfo BrokerWireFormat
        {
            get {
                return brokerWireFormatInfo;
            }
        }
        
        // Implementation methods
        
        /// <summary>
        /// Performs a synchronous request-response with the broker
        /// </summary>
        public Response SyncRequest(Command command)
        {
            CheckConnected();
            Response response = transport.Request(command);
            if (response is ExceptionResponse)
            {
                ExceptionResponse exceptionResponse = (ExceptionResponse) response;
                // TODO include stack trace
                throw new OpenWireException("Request failed: " + exceptionResponse);
            }
            return response;
        }
        
        public void OneWay(Command command)
        {
            CheckConnected();
            transport.Oneway(command);
        }
        
        public void DisposeOf(DataStructure objectId)
        {
            RemoveInfo command = new RemoveInfo();
            command.ObjectId = objectId;
            SyncRequest(command);
        }
        
        
        /// <summary>
        /// Creates a new temporary destination name
        /// </summary>
        public String CreateTemporaryDestinationName()
        {
            lock (this)
            {
                return info.ConnectionId.Value + ":" + (++temporaryDestinationCounter);
            }
        }
        
        /// <summary>
        /// Creates a new local transaction ID
        /// </summary>
        public LocalTransactionId CreateLocalTransactionId()
        {
            LocalTransactionId id= new LocalTransactionId();
            id.ConnectionId = ConnectionId;
            lock (this)
            {
                id.Value = (++localTransactionCounter);
            }
            return id;
        }
        
        protected void CheckConnected()
        {
            if (closed)
            {
                throw new ConnectionClosedException();
            }
            if (!connected)
            {
                connected = true;
                // now lets send the connection and see if we get an ack/nak
                SyncRequest(info);
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
            else if (command is WireFormatInfo)
            {
                this.brokerWireFormatInfo = (WireFormatInfo) command;
            }
            else if (command is BrokerInfo)
            {
                this.brokerInfo = (BrokerInfo) command;
            }
            else
            {
                Console.WriteLine("ERROR:ÊUnknown command: " + command);
            }
        }
        
        protected SessionInfo CreateSessionInfo(AcknowledgementMode acknowledgementMode)
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
        
    }
}
