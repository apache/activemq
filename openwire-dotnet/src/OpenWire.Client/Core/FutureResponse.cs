using System;
using System.Threading;

using OpenWire.Client;
using OpenWire.Client.Commands;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// Handles asynchronous responses
    /// </summary>
    public class FutureResponse : IAsyncResult
    {
        
        private Response response;
        private Mutex asyncWaitHandle = new Mutex();
        private Object semaphore = new Object();
        private int maxWait = 3000;
        private bool isCompleted;
        
        public WaitHandle AsyncWaitHandle
        {
            get { return asyncWaitHandle; }
        }
        
        public object AsyncState
        {
            get { return response; }
            set { Response = (Response) value; }
        }
        
        public bool IsCompleted
        {
            get { return isCompleted; }
        }
        
        public bool CompletedSynchronously
        {
            get { return false; }
        }
        
        public Response Response
        {
            // Blocks the caller until a value has been set
            get {
                while (response == null)
                {
                    try {
                    lock (semaphore)
                    {
                        Monitor.Wait(semaphore, maxWait);
                    }
                    }
                    catch (Exception e) {
                        Console.WriteLine("Caught while waiting on monitor: " + e);
                    }
                }
                return response;
            }
            set {
                lock (semaphore)
                {
                    response = value;
                    isCompleted = true;
                    Monitor.PulseAll(semaphore);
                }
            }
        }
    }
}
