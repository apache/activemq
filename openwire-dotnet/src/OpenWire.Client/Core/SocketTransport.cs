using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;


using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core
{
    
    /// <summary>
    /// An implementation of ITransport that uses sockets to communicate with the broker
    /// </summary>
    public class SocketTransport : ITransport
    {
        private readonly object transmissionLock = new object();
        private Socket socket;
        private OpenWireFormat wireformat = new OpenWireFormat();
        private BinaryReader socketReader;
        private BinaryWriter socketWriter;
        private Thread readThread;
        private bool closed;
        private IDictionary requestMap = new Hashtable(); // TODO threadsafe
        private short nextCommandId;
        private bool started;
        
        public event CommandHandler Command;
        public event ExceptionHandler Exception;
        
        
        
        public SocketTransport(string host, int port)
        {
            //Console.WriteLine("Opening socket to: " + host + " on port: " + port);
            socket = Connect(host, port);
        }
        
        /// <summary>
        /// Method Start
        /// </summary>
        public void Start()
        {
            if (!started)
            {
                started = true;
                
                NetworkStream networkStream = new NetworkStream(socket);
                socketWriter = new BinaryWriter(networkStream);
                socketReader = new BinaryReader(networkStream);
                /*
                 socketWriter = new BinaryWriter(new NetworkStream(socket));
                 socketReader = new BinaryReader(new NetworkStream(socket));
                 */
                
                // now lets create the background read thread
                readThread = new Thread(new ThreadStart(ReadLoop));
                readThread.Start();
            }
        }
        
        
        public void Oneway(Command command)
        {
            command.CommandId = GetNextCommandId();
            command.ResponseRequired = false;
            Send(command);
        }
        
        public FutureResponse AsyncRequest(Command command)
        {
            command.CommandId = GetNextCommandId();
            command.ResponseRequired = true;
            Send(command);
            FutureResponse future = new FutureResponse();
            requestMap[command.CommandId] = future;
            return future;
        }
        
        public Response Request(Command command)
        {
            FutureResponse response = AsyncRequest(command);
            return response.Response;
        }
        
        public void Dispose()
        {
            lock (transmissionLock)
            {
                socket.Close();
                closed = true;
            }
            socketWriter.Close();
            socketReader.Close();
        }
        
        public void ReadLoop()
        {
            while (!closed)
            {
                Command command = null;
                try
                {
                    command = (Command) wireformat.Unmarshal(socketReader);
                }
                catch (EndOfStreamException e)
                {
                    // stream closed
                    break;
                }
                catch (ObjectDisposedException e)
                {
                    // stream closed
                    break;
                }
                catch (IOException e)
                {
                    // error, assume closing
                    break;
                }
                if (command is Response)
                {
                    Response response = (Response) command;
                    FutureResponse future = (FutureResponse) requestMap[response.CorrelationId];
                    if (future != null)
                    {
                        if (response is ExceptionResponse)
                        {
                            ExceptionResponse er = (ExceptionResponse) response;
                            Exception e = new BrokerException(er.Exception);
                            if (this.Exception != null)
                            {
                                this.Exception(this, e);
                            }
                            else
                            {
                                throw e;
                            }
                        }
                        else
                        {
                            future.Response = response;
                        }
                    }
                    else
                    {
                        Console.WriteLine("ERROR: Unknown response ID: " + response.CommandId + " for response: " + response);
                    }
                }
                else
                {
                    if (this.Command != null)
                    {
                        this.Command(this, command);
                    }
                    else
                    {
                        Console.WriteLine("ERROR: No handler available to process command: " + command);
                    }
                }
            }
        }
        
        
        // Implementation methods
        
        protected void Send(Command command)
        {
            lock (transmissionLock)
            {
                //Console.WriteLine("Sending command: " + command  + " with ID: " + command.CommandId + " response: " + command.ResponseRequired);
                
                wireformat.Marshal(command, socketWriter);
                socketWriter.Flush();
            }
        }
        
        protected short GetNextCommandId()
        {
            lock (transmissionLock)
            {
                return++nextCommandId;
            }
        }
        
        protected Socket Connect(string host, int port)
        {
            // Looping through the AddressList allows different type of connections to be tried
            // (IPv4, IPv6 and whatever else may be available).
            IPHostEntry hostEntry = Dns.Resolve(host);
            foreach (IPAddress address in hostEntry.AddressList)
            {
                Socket socket = new Socket(
                    address.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp);
                socket.Connect(new IPEndPoint(address, port));
                if (socket.Connected)
                {
                    return socket;
                }
            }
            throw new SocketException();
        }
    }
}

