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
package org.activeio.packet.async.vmpipe;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.activeio.packet.Packet;
import org.activeio.packet.async.AsyncChannel;
import org.activeio.packet.async.AsyncChannelFactory;
import org.activeio.packet.async.AsyncChannelListener;
import org.activeio.packet.async.AsyncChannelServer;

/**
 * 
 * @version $Revision$
 */
final public class VMPipeAsyncChannelFactory implements AsyncChannelFactory {
    
    //
    // We do all this crazy stuff of looking the server map using System
    // properties
    // because this class could be loaded multiple times in different
    // classloaders.
    //
    private static final String SERVER_MAP_LOCATION = VMPipeAsyncChannelFactory.class.getName() + ".SERVER_MAP";

    private static final Map SERVER_MAP;
    static {
        Map m = null;
        m = (Map) System.getProperties().get(SERVER_MAP_LOCATION);
        if (m == null) {
            m = Collections.synchronizedMap(new HashMap());
            System.getProperties().put(SERVER_MAP_LOCATION, m);
        }
        SERVER_MAP = m;
    }

    private final static ClassLoader MY_CLASSLOADER = Packet.class.getClassLoader();
    
    
    /**
     * Used to marshal calls to a PipeChannel in a different classloader.
     */
    static public class ClassloaderAsyncChannelAdapter implements AsyncChannel {

        private final ClassLoader cl;
        private final Object channel;
        private final Method writeMethod;
        private final Method setListenerMethod;
        private final Class listenerClazz;
        private final Class packetClazz;
        private final Object listenerProxy;
        private final Method duplicateMethod;
        private final Method startMethod;
        private final Method stopMethod;
        private final Method disposeMethod;

        private AsyncChannelListener channelListener;

        public class ListenerProxyHandler implements InvocationHandler {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                switch (method.getName().length()) {
                case 8: // onPacket
                    Object packet = duplicateMethod.invoke(args[0], new Object[]{MY_CLASSLOADER});  
                    channelListener.onPacket((Packet) packet);
                    break;
                case 13: // onPacketError
                    channelListener.onPacketError((IOException) args[0]);
                    break;
                default:
                    channelListener.onPacketError(new IOException("Unknown proxy method invocation: "+method.getName()));
                }
                return null;
            }
        }

        public ClassloaderAsyncChannelAdapter(Object channel) throws SecurityException, NoSuchMethodException,
                ClassNotFoundException {
            this.channel = channel;
            Class clazz = channel.getClass();
            cl = clazz.getClassLoader();

            listenerClazz = cl.loadClass(AsyncChannelListener.class.getName());
            packetClazz = cl.loadClass(Packet.class.getName());
            writeMethod = clazz.getMethod("write", new Class[] { packetClazz });
            startMethod = clazz.getMethod("start", new Class[] { });
            stopMethod = clazz.getMethod("stop", new Class[] {});
            disposeMethod = clazz.getMethod("dispose", new Class[] { });

            setListenerMethod = clazz.getMethod("setAsyncChannelListener", new Class[] { listenerClazz });
            duplicateMethod = packetClazz.getMethod("duplicate", new Class[] { ClassLoader.class });

            ListenerProxyHandler handler = new ListenerProxyHandler();
            listenerProxy = Proxy.newProxyInstance(cl, new Class[] { listenerClazz }, handler);
        }

        public void write(Packet packet) throws IOException {
            callIOExceptionMethod(writeMethod, new Object[] { packet.duplicate(cl) });
        }

        public void setAsyncChannelListener(AsyncChannelListener channelListener) {
            this.channelListener = channelListener;
            callMethod(setListenerMethod, new Object[] { channelListener == null ? null : listenerProxy });
        }

        public AsyncChannelListener getAsyncChannelListener() {
            return channelListener;
        }

        public void dispose() {
            callMethod(disposeMethod, new Object[] { });
        }

        public void start() throws IOException {
            callIOExceptionMethod(startMethod, new Object[] {});
        }

        public void stop() throws IOException {
            callIOExceptionMethod(stopMethod, new Object[] {});
        }
        
        private void callMethod(Method method, Object[] args) {
            try {
                method.invoke(channel, args);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof RuntimeException) {
                    throw (RuntimeException) e.getTargetException();
                }
                throw new RuntimeException(e.getTargetException());
            } catch (Throwable e) {
                throw new RuntimeException("Reflexive invocation failed: " + e, e);
            }            
        }
        
        private void callIOExceptionMethod(Method method, Object[] args) throws IOException {
            try {
                method.invoke(channel, args);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IOException) {
                    throw (IOException) e.getTargetException();
                }
                if (e.getTargetException() instanceof RuntimeException) {
                    throw (RuntimeException) e.getTargetException();
                }
                throw new RuntimeException(e.getTargetException());
            } catch (Throwable e) {
                throw (IOException) new IOException("Reflexive invocation failed: " + e).initCause(e);
            }            
        }

        //
        // The following methods do not need to delegate since they
        // are implemented as noops in the PipeChannel
        //
        public Object getAdapter(Class target) {
            if (target.isAssignableFrom(getClass())) {
                return this;
            }
            return null;
        }

        public void flush() throws IOException {
        }

    }

    private boolean forceRefelection;

    public AsyncChannel openAsyncChannel(URI location) throws IOException {

        Object server = lookupServer(location);
        if (!forceRefelection && server.getClass() == VMPipeAsyncChannelServer.class) {
            return ((VMPipeAsyncChannelServer) server).connect();
        }

        // Asume server is in a different classloader.
        // Use reflection to connect.
        try {
            Method method = server.getClass().getMethod("connect", new Class[] {});
            Object channel = method.invoke(server, new Object[] {});
            return new ClassloaderAsyncChannelAdapter(channel);
        } catch (Throwable e) {
            throw (IOException) new IOException("Connection could not be established: " + e).initCause(e);
        }
    }

    public AsyncChannelServer bindAsyncChannel(URI bindURI) throws IOException {
        VMPipeAsyncChannelServer server = new VMPipeAsyncChannelServer(bindURI);
        bindServer(bindURI, server);
        return server;
    }

    private static Map getServerMap() {
        return SERVER_MAP;
    }

    static public String getServerKeyForURI(URI location) {
        return location.getHost();
    }

    public static void bindServer(URI bindURI, VMPipeAsyncChannelServer server) throws IOException {
        String key = getServerKeyForURI(bindURI);
        if (getServerMap().get(key) != null)
            throw new IOException("Server is allready bound at: " + bindURI);
        getServerMap().put(key, server);
    }

    public static Object lookupServer(URI location) throws IOException {
        String key = getServerKeyForURI(location);
        Object server = getServerMap().get(key);
        if (server == null) {
            throw new IOException("Connection refused.");
        }
        return server;
    }

    public static void unbindServer(URI bindURI) {
        String key = getServerKeyForURI(bindURI);
        getServerMap().remove(key);
    }

    public boolean isForceRefelection() {
        return forceRefelection;
    }
    
    public void setForceRefelection(boolean forceRefelection) {
        this.forceRefelection = forceRefelection;
    }
    
}
