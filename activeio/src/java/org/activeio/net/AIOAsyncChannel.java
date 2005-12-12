/** 
 * 
 * Copyright 2004 Hiram Chirino
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/

package org.activeio.net;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;

import org.activeio.AsyncChannel;
import org.activeio.AsyncChannelListener;
import org.activeio.Packet;
import org.activeio.packet.ByteBufferPacket;
import org.activeio.packet.EOSPacket;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.io.async.AsyncSocketChannel;
import com.ibm.io.async.IAbstractAsyncFuture;
import com.ibm.io.async.IAsyncFuture;
import com.ibm.io.async.ICompletionListener;

/**
 * @version $Revision$
 */
final public class AIOAsyncChannel implements AsyncChannel, ICompletionListener, SocketMetadata {

    protected static final int DEFAULT_BUFFER_SIZE = ByteBufferPacket.DEFAULT_DIRECT_BUFFER_SIZE;

    private final AsyncSocketChannel socketChannel;
    private final Socket socket;

    private AsyncChannelListener channelListener;
    private ByteBuffer inputByteBuffer;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private CountDownLatch doneCountDownLatch;

    protected AIOAsyncChannel(AsyncSocketChannel socketChannel) throws IOException {
        this.socketChannel = socketChannel;
        this.socket = socketChannel.socket();
        this.socket.setSendBufferSize(DEFAULT_BUFFER_SIZE);
        this.socket.setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        this.socket.setSoTimeout(0);
    }
    
    private ByteBuffer allocateBuffer() {
        return ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
    }   

    public void setAsyncChannelListener(AsyncChannelListener channelListener) {
        this.channelListener = channelListener;
    }

    public AsyncChannelListener getAsyncChannelListener() {
        return channelListener;
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }

    public void dispose() {
        if( running.get() && channelListener!=null ) {
            channelListener.onPacketError(new SocketException("Socket closed."));
        }
        try {
            stop(NO_WAIT_TIMEOUT);
        } catch (IOException e) {
        }
        try {
            socketChannel.close();
        } catch (IOException e) {
        }
    }

    public void start() throws IOException {
        if( running.compareAndSet(false, true) ) {
            doneCountDownLatch = new CountDownLatch(1);
            requestNextRead();
        }
    }

    public void stop(long timeout) throws IOException {
        if( running.compareAndSet(true, false) ) {
            try {
                if( timeout == NO_WAIT_TIMEOUT ) {
                    doneCountDownLatch.await(0, TimeUnit.MILLISECONDS);
                } else if( timeout == WAIT_FOREVER_TIMEOUT ) {
                    doneCountDownLatch.await();
                } else {
                    doneCountDownLatch.await(timeout,  TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
    }

    public void write(Packet packet) throws IOException {
        ByteBuffer data = ((ByteBufferPacket)packet).getByteBuffer();
        while( data.hasRemaining() ) {
	        IAsyncFuture future = socketChannel.write(data);
	        try {
	            future.getByteCount();
	        } catch (InterruptedException e) {
	            throw new InterruptedIOException();
	        }
        }
    }

    public void flush() throws IOException {
    }

    public void futureCompleted(IAbstractAsyncFuture abstractFuture, Object attribute) {
        IAsyncFuture future = (IAsyncFuture)abstractFuture;
        try {
            
            if( inputByteBuffer.position()>0 ) {
	            ByteBuffer remaining = inputByteBuffer.slice();            
	            Packet data = new ByteBufferPacket(((ByteBuffer)inputByteBuffer.flip()).slice());
	            
	            channelListener.onPacket(data);	            
	            // Keep the remaining buffer around to fill with data.
	            inputByteBuffer = remaining;
	            requestNextRead();
	            
            } else {                
                channelListener.onPacket(EOSPacket.EOS_PACKET);  
            }
            
        } catch (IOException e) {
            channelListener.onPacketError(e);
        }
    }

    private void requestNextRead() throws InterruptedIOException {
        
        // Don't do next read if we have stopped running.
        if( !running.get() ) {
            doneCountDownLatch.countDown();
            return;
        }
        
        try {
            
            if( inputByteBuffer==null || !inputByteBuffer.hasRemaining() ) {
                inputByteBuffer = allocateBuffer();
            }

            IAsyncFuture future = socketChannel.read(inputByteBuffer);
            future.addCompletionListener(this, null, false);
            
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

    }

    public InetAddress getInetAddress() {
        return socket.getInetAddress();
    }
    public boolean getKeepAlive() throws SocketException {
        return socket.getKeepAlive();
    }
    public InetAddress getLocalAddress() {
        return socket.getLocalAddress();
    }
    public int getLocalPort() {
        return socket.getLocalPort();
    }
    public SocketAddress getLocalSocketAddress() {
        return socket.getLocalSocketAddress();
    }
    public boolean getOOBInline() throws SocketException {
        return socket.getOOBInline();
    }
    public int getPort() {
        return socket.getPort();
    }
    public int getReceiveBufferSize() throws SocketException {
        return socket.getReceiveBufferSize();
    }
    public SocketAddress getRemoteSocketAddress() {
        return socket.getRemoteSocketAddress();
    }
    public boolean getReuseAddress() throws SocketException {
        return socket.getReuseAddress();
    }
    public int getSendBufferSize() throws SocketException {
        return socket.getSendBufferSize();
    }
    public int getSoLinger() throws SocketException {
        return socket.getSoLinger();
    }
    public int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }
    public boolean getTcpNoDelay() throws SocketException {
        return socket.getTcpNoDelay();
    }
    public int getTrafficClass() throws SocketException {
        return socket.getTrafficClass();
    }
    public boolean isBound() {
        return socket.isBound();
    }
    public boolean isClosed() {
        return socket.isClosed();
    }
    public boolean isConnected() {
        return socket.isConnected();
    }
    public void setKeepAlive(boolean on) throws SocketException {
        socket.setKeepAlive(on);
    }
    public void setOOBInline(boolean on) throws SocketException {
        socket.setOOBInline(on);
    }
    public void setReceiveBufferSize(int size) throws SocketException {
        socket.setReceiveBufferSize(size);
    }
    public void setReuseAddress(boolean on) throws SocketException {
        socket.setReuseAddress(on);
    }
    public void setSendBufferSize(int size) throws SocketException {
        socket.setSendBufferSize(size);
    }
    public void setSoLinger(boolean on, int linger) throws SocketException {
        socket.setSoLinger(on, linger);
    }
    public void setTcpNoDelay(boolean on) throws SocketException {
        socket.setTcpNoDelay(on);
    }
    public void setTrafficClass(int tc) throws SocketException {
        socket.setTrafficClass(tc);
    }
    public String toString() {
        return "AIO Connection: "+getLocalSocketAddress()+" -> "+getRemoteSocketAddress();
    }
 }