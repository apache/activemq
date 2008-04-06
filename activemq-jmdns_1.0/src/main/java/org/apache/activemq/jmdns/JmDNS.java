///Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL


package org.apache.activemq.jmdns;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

// REMIND: multiple IP addresses

/**
 * mDNS implementation in Java.
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff, Rick Blair, Jeff Sonstein,
 * Werner Randelshofer, Pierre Frisch, Scott Lewis
 */
public class JmDNS
{
    private static Logger logger = Logger.getLogger(JmDNS.class.toString());
    /**
     * The version of JmDNS.
     */
    public static String VERSION = "2.0";

    /**
     * This is the multicast group, we are listening to for multicast DNS messages.
     */
    private InetAddress group;
    /**
     * This is our multicast socket.
     */
    private MulticastSocket socket;

    /**
     * Used to fix live lock problem on unregester.
     */

     protected boolean closed = false;

    /**
     * Holds instances of JmDNS.DNSListener.
     * Must by a synchronized collection, because it is updated from
     * concurrent threads.
     */
    private List listeners;
    /**
     * Holds instances of ServiceListener's.
     * Keys are Strings holding a fully qualified service type.
     * Values are LinkedList's of ServiceListener's.
     */
    private Map serviceListeners;
    /**
     * Holds instances of ServiceTypeListener's.
     */
    private List typeListeners;


    /**
     * Cache for DNSEntry's.
     */
    private DNSCache cache;

    /**
     * This hashtable holds the services that have been registered.
     * Keys are instances of String which hold an all lower-case version of the
     * fully qualified service name.
     * Values are instances of ServiceInfo.
     */
    Map services;

    /**
     * This hashtable holds the service types that have been registered or
     * that have been received in an incoming datagram.
     * Keys are instances of String which hold an all lower-case version of the
     * fully qualified service type.
     * Values hold the fully qualified service type.
     */
    Map serviceTypes;
    /**
     * This is the shutdown hook, we registered with the java runtime.
     */
    private Thread shutdown;

    /**
     * Handle on the local host
     */
    HostInfo localHost;

    private Thread incomingListener = null;

    /**
     * Throttle count.
     * This is used to count the overall number of probes sent by JmDNS.
     * When the last throttle increment happened .
     */
    private int throttle;
    /**
     * Last throttle increment.
     */
    private long lastThrottleIncrement;

    /**
     * The timer is used to dispatch all outgoing messages of JmDNS.
     * It is also used to dispatch maintenance tasks for the DNS cache.
     */
    private Timer timer;

    /**
     * The source for random values.
     * This is used to introduce random delays in responses. This reduces the
     * potential for collisions on the network.
     */
    private final static Random random = new Random();

    /**
     * This lock is used to coordinate processing of incoming and outgoing
     * messages. This is needed, because the Rendezvous Conformance Test
     * does not forgive race conditions.
     */
    private Object ioLock = new Object();

    /**
     * If an incoming package which needs an answer is truncated, we store it
     * here. We add more incoming DNSRecords to it, until the JmDNS.Responder
     * timer picks it up.
     * Remind: This does not work well with multiple planned answers for packages
     * that came in from different clients.
     */
    private DNSIncoming plannedAnswer;
    
    // State machine
    /**
     * The state of JmDNS.
     * <p/>
     * For proper handling of concurrency, this variable must be
     * changed only using methods advanceState(), revertState() and cancel().
     */
    private DNSState state = DNSState.PROBING_1;

    /**
     * Timer task associated to the host name.
     * This is used to prevent from having multiple tasks associated to the host
     * name at the same time.
     */
    TimerTask task;

    /**
     * This hashtable is used to maintain a list of service types being collected
     * by this JmDNS instance.
     * The key of the hashtable is a service type name, the value is an instance
     * of JmDNS.ServiceCollector.
     *
     * @see #list
     */
    private HashMap serviceCollectors = new HashMap();

    /**
     * Create an instance of JmDNS.
     */
    public JmDNS() throws IOException
    {
        logger.finer("JmDNS instance created");
        try
        {
            InetAddress addr = InetAddress.getLocalHost();
            init(addr.isLoopbackAddress() ? null : addr, addr.getHostName()); // [PJYF Oct 14 2004] Why do we disallow the loopback address?
        }
        catch (IOException e)
        {
            init(null, "computer");
        }
    }

    /**
     * Create an instance of JmDNS and bind it to a
     * specific network interface given its IP-address.
     */
    public JmDNS(InetAddress addr) throws IOException
    {
        try
        {
            init(addr, addr.getHostName());
        }
        catch (IOException e)
        {
            init(null, "computer");
        }
    }

    /**
     * Initialize everything.
     *
     * @param address The interface to which JmDNS binds to.
     * @param name    The host name of the interface.
     */
    private void init(InetAddress address, String name) throws IOException
    {
        // A host name with "." is illegal. so strip off everything and append .local.
        int idx = name.indexOf(".");
        if (idx > 0)
        {
            name = name.substring(0, idx);
        }
        name += ".local.";
        // localHost to IP address binding
        localHost = new HostInfo(address, name);

        cache = new DNSCache(100);

        listeners = Collections.synchronizedList(new ArrayList());
        serviceListeners = new HashMap();
        typeListeners = new ArrayList();

        services = new Hashtable(20);
        serviceTypes = new Hashtable(20);

        // REMIND: If I could pass in a name for the Timer thread,
        //         I would pass 'JmDNS.Timer'.
        timer = new Timer();
        new RecordReaper().start();
        shutdown = new Thread(new Shutdown(), "JmDNS.Shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);

        incomingListener = new Thread(new SocketListener(), "JmDNS.SocketListener");

        // Bind to multicast socket
        openMulticastSocket(localHost);
        start(services.values());
    }

    private void start(Collection serviceInfos)
    {
        state = DNSState.PROBING_1;
        incomingListener.start();
        new Prober().start();
        for (Iterator iterator = serviceInfos.iterator(); iterator.hasNext();)
        {
            try
            {
                registerService(new ServiceInfo((ServiceInfo) iterator.next()));
            }
            catch (Exception exception)
            {
                logger.log(Level.WARNING, "start() Registration exception ", exception);
            }
        }
    }

    private void openMulticastSocket(HostInfo hostInfo) throws IOException
    {
        if (group == null)
        {
            group = InetAddress.getByName(DNSConstants.MDNS_GROUP);
        }
        if (socket != null)
        {
            this.closeMulticastSocket();
        }
        socket = new MulticastSocket(DNSConstants.MDNS_PORT);
        if ((hostInfo != null) && (localHost.getInterface() != null))
        {
            socket.setNetworkInterface(hostInfo.getInterface());
        }
        socket.setTimeToLive(255);
        socket.joinGroup(group);
    }

    private void closeMulticastSocket()
    {
        logger.finer("closeMulticastSocket()");
        if (socket != null)
        {
            // close socket
            try
            {
                socket.leaveGroup(group);
                socket.close();
                if (incomingListener != null)
                {
                    incomingListener.join();
                }
            }
            catch (Exception exception)
            {
                logger.log(Level.WARNING, "closeMulticastSocket() Close socket exception ", exception);
            }
            socket = null;
        }
    }
    
    // State machine
    /**
     * Sets the state and notifies all objects that wait on JmDNS.
     */
    synchronized void advanceState()
    {
        state = state.advance();
        notifyAll();
    }

    /**
     * Sets the state and notifies all objects that wait on JmDNS.
     */
    synchronized void revertState()
    {
        state = state.revert();
        notifyAll();
    }

    /**
     * Sets the state and notifies all objects that wait on JmDNS.
     */
    synchronized void cancel()
    {
        state = DNSState.CANCELED;
        notifyAll();
    }

    /**
     * Returns the current state of this info.
     */
    DNSState getState()
    {
        return state;
    }


    /**
     * Return the DNSCache associated with the cache variable
     */
    DNSCache getCache()
    {
        return cache;
    }

    /**
     * Return the HostName associated with this JmDNS instance.
     * Note: May not be the same as what started.  The host name is subject to
     * negotiation.
     */
    public String getHostName()
    {
        return localHost.getName();
    }

    public HostInfo getLocalHost()
    {
        return localHost;
    }

    /**
     * Return the address of the interface to which this instance of JmDNS is
     * bound.
     */
    public InetAddress getInterface() throws IOException
    {
        return socket.getInterface();
    }

    /**
     * Get service information. If the information is not cached, the method
     * will block until updated information is received.
     * <p/>
     * Usage note: Do not call this method from the AWT event dispatcher thread.
     * You will make the user interface unresponsive.
     *
     * @param type fully qualified service type, such as <code>_http._tcp.local.</code> .
     * @param name unqualified service name, such as <code>foobar</code> .
     * @return null if the service information cannot be obtained
     */
    public ServiceInfo getServiceInfo(String type, String name)
    {
        return getServiceInfo(type, name, 3 * 1000);
    }

    /**
     * Get service information. If the information is not cached, the method
     * will block for the given timeout until updated information is received.
     * <p/>
     * Usage note: If you call this method from the AWT event dispatcher thread,
     * use a small timeout, or you will make the user interface unresponsive.
     *
     * @param type    full qualified service type, such as <code>_http._tcp.local.</code> .
     * @param name    unqualified service name, such as <code>foobar</code> .
     * @param timeout timeout in milliseconds
     * @return null if the service information cannot be obtained
     */
    public ServiceInfo getServiceInfo(String type, String name, int timeout)
    {
        ServiceInfo info = new ServiceInfo(type, name);
        new ServiceInfoResolver(info).start();

        try
        {
            long end = System.currentTimeMillis() + timeout;
            long delay;
            synchronized (info)
            {
                while (!info.hasData() && (delay = end - System.currentTimeMillis()) > 0)
                {
                    info.wait(delay);
                }
            }
        }
        catch (InterruptedException e)
        {
            // empty
        }

        return (info.hasData()) ? info : null;
    }

    /**
     * Request service information. The information about the service is
     * requested and the ServiceListener.resolveService method is called as soon
     * as it is available.
     * <p/>
     * Usage note: Do not call this method from the AWT event dispatcher thread.
     * You will make the user interface unresponsive.
     *
     * @param type full qualified service type, such as <code>_http._tcp.local.</code> .
     * @param name unqualified service name, such as <code>foobar</code> .
     */
    public void requestServiceInfo(String type, String name)
    {
        requestServiceInfo(type, name, 3 * 1000);
    }

    /**
     * Request service information. The information about the service is requested
     * and the ServiceListener.resolveService method is called as soon as it is available.
     *
     * @param type    full qualified service type, such as <code>_http._tcp.local.</code> .
     * @param name    unqualified service name, such as <code>foobar</code> .
     * @param timeout timeout in milliseconds
     */
    public void requestServiceInfo(String type, String name, int timeout)
    {
        registerServiceType(type);
        ServiceInfo info = new ServiceInfo(type, name);
        new ServiceInfoResolver(info).start();

        try
        {
            long end = System.currentTimeMillis() + timeout;
            long delay;
            synchronized (info)
            {
                while (!info.hasData() && (delay = end - System.currentTimeMillis()) > 0)
                {
                    info.wait(delay);
                }
            }
        }
        catch (InterruptedException e)
        {
            // empty
        }
    }

    void handleServiceResolved(ServiceInfo info)
    {
        List list = (List) serviceListeners.get(info.type.toLowerCase());
        if (list != null)
        {
            ServiceEvent event = new ServiceEvent(this, info.type, info.getName(), info);
            // Iterate on a copy in case listeners will modify it
            final ArrayList listCopy = new ArrayList(list);
            for (Iterator iterator = listCopy.iterator(); iterator.hasNext();)
            {
                ((ServiceListener) iterator.next()).serviceResolved(event);
            }
        }
    }

    /**
     * Listen for service types.
     *
     * @param listener listener for service types
     */
    public void addServiceTypeListener(ServiceTypeListener listener) throws IOException
    {
        synchronized (this)
        {
            typeListeners.remove(listener);
            typeListeners.add(listener);
        }

        // report cached service types
        for (Iterator iterator = serviceTypes.values().iterator(); iterator.hasNext();)
        {
            listener.serviceTypeAdded(new ServiceEvent(this, (String) iterator.next(), null, null));
        }

        new TypeResolver().start();
    }

    /**
     * Remove listener for service types.
     *
     * @param listener listener for service types
     */
    public void removeServiceTypeListener(ServiceTypeListener listener)
    {
        synchronized (this)
        {
            typeListeners.remove(listener);
        }
    }

    /**
     * Listen for services of a given type. The type has to be a fully qualified
     * type name such as <code>_http._tcp.local.</code>.
     *
     * @param type     full qualified service type, such as <code>_http._tcp.local.</code>.
     * @param listener listener for service updates
     */
    public void addServiceListener(String type, ServiceListener listener)
    {
        String lotype = type.toLowerCase();
        removeServiceListener(lotype, listener);
        List list = null;
        synchronized (this)
        {
            list = (List) serviceListeners.get(lotype);
            if (list == null)
            {
                list = Collections.synchronizedList(new LinkedList());
                serviceListeners.put(lotype, list);
            }
            list.add(listener);
        }

        // report cached service types
        for (Iterator i = cache.iterator(); i.hasNext();)
        {
            for (DNSCache.CacheNode n = (DNSCache.CacheNode) i.next(); n != null; n = n.next())
            {
                DNSRecord rec = (DNSRecord) n.getValue();
                if (rec.type == DNSConstants.TYPE_SRV)
                {
                    if (rec.name.endsWith(type))
                    {
                        listener.serviceAdded(new ServiceEvent(this, type, toUnqualifiedName(type, rec.name), null));
                    }
                }
            }
        }
        new ServiceResolver(type).start();
    }

    /**
     * Remove listener for services of a given type.
     *
     * @param listener listener for service updates
     */
    public void removeServiceListener(String type, ServiceListener listener)
    {
        type = type.toLowerCase();
        List list = (List) serviceListeners.get(type);
        if (list != null)
        {
            synchronized (this)
            {
                list.remove(listener);
                if (list.size() == 0)
                {
                    serviceListeners.remove(type);
                }
            }
        }
    }

    /**
     * Register a service. The service is registered for access by other jmdns clients.
     * The name of the service may be changed to make it unique.
     */
    public void registerService(ServiceInfo info) throws IOException
    {
        registerServiceType(info.type);

        // bind the service to this address
        info.server = localHost.getName();
        info.addr = localHost.getAddress();

        synchronized (this)
        {
            makeServiceNameUnique(info);
            services.put(info.getQualifiedName().toLowerCase(), info);
        }

        new /*Service*/Prober().start();
        try
        {
            synchronized (info)
            {
                while (info.getState().compareTo(DNSState.ANNOUNCED) < 0)
                {
                    info.wait();
                }
            }
        }
        catch (InterruptedException e)
        {
            //empty
        }
        logger.fine("registerService() JmDNS registered service as " + info);
    }

    /**
     * Unregister a service. The service should have been registered.
     */
    public void unregisterService(ServiceInfo info)
    {
        synchronized (this)
        {
            services.remove(info.getQualifiedName().toLowerCase());
        }
        info.cancel();

        // Note: We use this lock object to synchronize on it.
        //       Synchronizing on another object (e.g. the ServiceInfo) does
        //       not make sense, because the sole purpose of the lock is to
        //       wait until the canceler has finished. If we synchronized on
        //       the ServiceInfo or on the Canceler, we would block all
        //       accesses to synchronized methods on that object. This is not
        //       what we want!
        Object lock = new Object();
        new Canceler(info, lock).start();

        // Remind: We get a deadlock here, if the Canceler does not run!
        try
        {
            synchronized (lock)
            {
                lock.wait();
            }
        }
        catch (InterruptedException e)
        {
            // empty
        }
    }

    /**
     * Unregister all services.
     */
    public void unregisterAllServices()
    {
        logger.finer("unregisterAllServices()");
        if (services.size() == 0)
        {
            return;
        }

        Collection list;
        synchronized (this)
        {
            list = new LinkedList(services.values());
            services.clear();
        }
        for (Iterator iterator = list.iterator(); iterator.hasNext();)
        {
            ((ServiceInfo) iterator.next()).cancel();
        }


        Object lock = new Object();
        new Canceler(list, lock).start();
              // Remind: We get a livelock here, if the Canceler does not run!
        try {
            synchronized (lock) {
                if (!closed) {
                    lock.wait();
                }
            }
        } catch (InterruptedException e) {
            // empty
        }


    }

    /**
     * Register a service type. If this service type was not already known,
     * all service listeners will be notified of the new service type. Service types
     * are automatically registered as they are discovered.
     */
    public void registerServiceType(String type)
    {
        String name = type.toLowerCase();
        if (serviceTypes.get(name) == null)
        {
            if ((type.indexOf("._mdns._udp.") < 0) && !type.endsWith(".in-addr.arpa."))
            {
                Collection list;
                synchronized (this)
                {
                    serviceTypes.put(name, type);
                    list = new LinkedList(typeListeners);
                }
                for (Iterator iterator = list.iterator(); iterator.hasNext();)
                {
                    ((ServiceTypeListener) iterator.next()).serviceTypeAdded(new ServiceEvent(this, type, null, null));
                }
            }
        }
    }

    /**
     * Generate a possibly unique name for a host using the information we
     * have in the cache.
     *
     * @return returns true, if the name of the host had to be changed.
     */
    private boolean makeHostNameUnique(DNSRecord.Address host)
    {
        String originalName = host.getName();
        long now = System.currentTimeMillis();

        boolean collision;
        do
        {
            collision = false;

            // Check for collision in cache
            for (DNSCache.CacheNode j = cache.find(host.getName().toLowerCase()); j != null; j = j.next())
            {
                DNSRecord a = (DNSRecord) j.getValue();
                if (false)
                {
                    host.name = incrementName(host.getName());
                    collision = true;
                    break;
                }
            }
        }
        while (collision);

        if (originalName.equals(host.getName()))
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * Generate a possibly unique name for a service using the information we
     * have in the cache.
     *
     * @return returns true, if the name of the service info had to be changed.
     */
    private boolean makeServiceNameUnique(ServiceInfo info)
    {
        String originalQualifiedName = info.getQualifiedName();
        long now = System.currentTimeMillis();

        boolean collision;
        do
        {
            collision = false;

            // Check for collision in cache
            for (DNSCache.CacheNode j = cache.find(info.getQualifiedName().toLowerCase()); j != null; j = j.next())
            {
                DNSRecord a = (DNSRecord) j.getValue();
                if ((a.type == DNSConstants.TYPE_SRV) && !a.isExpired(now))
                {
                    DNSRecord.Service s = (DNSRecord.Service) a;
                    if (s.port != info.port || !s.server.equals(localHost.getName()))
                    {
                        logger.finer("makeServiceNameUnique() JmDNS.makeServiceNameUnique srv collision:" + a + " s.server=" + s.server + " " + localHost.getName() + " equals:" + (s.server.equals(localHost.getName())));
                        info.setName(incrementName(info.getName()));
                        collision = true;
                        break;
                    }
                }
            }

            // Check for collision with other service infos published by JmDNS
            Object selfService = services.get(info.getQualifiedName().toLowerCase());
            if (selfService != null && selfService != info)
            {
                info.setName(incrementName(info.getName()));
                collision = true;
            }
        }
        while (collision);

        return !(originalQualifiedName.equals(info.getQualifiedName()));
    }

    String incrementName(String name)
    {
        try
        {
            int l = name.lastIndexOf('(');
            int r = name.lastIndexOf(')');
            if ((l >= 0) && (l < r))
            {
                name = name.substring(0, l) + "(" + (Integer.parseInt(name.substring(l + 1, r)) + 1) + ")";
            }
            else
            {
                name += " (2)";
            }
        }
        catch (NumberFormatException e)
        {
            name += " (2)";
        }
        return name;
    }

    /**
     * Add a listener for a question. The listener will receive updates
     * of answers to the question as they arrive, or from the cache if they
     * are already available.
     */
    void addListener(DNSListener listener, DNSQuestion question)
    {
        long now = System.currentTimeMillis();

        // add the new listener
        synchronized (this)
        {
            listeners.add(listener);
        }

        // report existing matched records
        if (question != null)
        {
            for (DNSCache.CacheNode i = cache.find(question.name); i != null; i = i.next())
            {
                DNSRecord c = (DNSRecord) i.getValue();
                if (question.answeredBy(c) && !c.isExpired(now))
                {
                    listener.updateRecord(this, now, c);
                }
            }
        }
    }

    /**
     * Remove a listener from all outstanding questions. The listener will no longer
     * receive any updates.
     */
    void removeListener(DNSListener listener)
    {
        synchronized (this)
        {
            listeners.remove(listener);
        }
    }
    
    
    // Remind: Method updateRecord should receive a better name.
    /**
     * Notify all listeners that a record was updated.
     */
    void updateRecord(long now, DNSRecord rec)
    {
        // We do not want to block the entire DNS while we are updating the record for each listener (service info)
        List listenerList = null;
        synchronized (this)
        {
            listenerList = new ArrayList(listeners);
        }
        for (Iterator iterator = listenerList.iterator(); iterator.hasNext();)
        {
            DNSListener listener = (DNSListener) iterator.next();
            listener.updateRecord(this, now, rec);
        }
        if (rec.type == DNSConstants.TYPE_PTR || rec.type == DNSConstants.TYPE_SRV)
        {
            List serviceListenerList = null;
            synchronized (this)
            {
                serviceListenerList = (List) serviceListeners.get(rec.name.toLowerCase());
                // Iterate on a copy in case listeners will modify it
                if (serviceListenerList != null)
                {
                    serviceListenerList = new ArrayList(serviceListenerList);
                }
            }
            if (serviceListenerList != null)
            {
                boolean expired = rec.isExpired(now);
                String type = rec.getName();
                String name = ((DNSRecord.Pointer) rec).getAlias();
                // DNSRecord old = (DNSRecord)services.get(name.toLowerCase());
                if (!expired)
                {
                    // new record
                    ServiceEvent event = new ServiceEvent(this, type, toUnqualifiedName(type, name), null);
                    for (Iterator iterator = serviceListenerList.iterator(); iterator.hasNext();)
                    {
                        ((ServiceListener) iterator.next()).serviceAdded(event);
                    }
                }
                else
                {
                    // expire record
                    ServiceEvent event = new ServiceEvent(this, type, toUnqualifiedName(type, name), null);
                    for (Iterator iterator = serviceListenerList.iterator(); iterator.hasNext();)
                    {
                        ((ServiceListener) iterator.next()).serviceRemoved(event);
                    }
                }
            }
        }
    }

    /**
     * Handle an incoming response. Cache answers, and pass them on to
     * the appropriate questions.
     */
    private void handleResponse(DNSIncoming msg) throws IOException
    {
        long now = System.currentTimeMillis();

        boolean hostConflictDetected = false;
        boolean serviceConflictDetected = false;

        for (Iterator i = msg.answers.iterator(); i.hasNext();)
        {
            boolean isInformative = false;
            DNSRecord rec = (DNSRecord) i.next();
            boolean expired = rec.isExpired(now);

            // update the cache
            DNSRecord c = (DNSRecord) cache.get(rec);
            if (c != null)
            {
                if (expired)
                {
                    isInformative = true;
                    cache.remove(c);
                }
                else
                {
                    c.resetTTL(rec);
                    rec = c;
                }
            }
            else
            {
                if (!expired)
                {
                    isInformative = true;
                    cache.add(rec);
                }
            }
            switch (rec.type)
            {
                case DNSConstants.TYPE_PTR:
                    // handle _mdns._udp records
                    if (rec.getName().indexOf("._mdns._udp.") >= 0)
                    {
                        if (!expired && rec.name.startsWith("_services._mdns._udp."))
                        {
                            isInformative = true;
                            registerServiceType(((DNSRecord.Pointer) rec).alias);
                        }
                        continue;
                    }
                    registerServiceType(rec.name);
                    break;
            }

            if ((rec.getType() == DNSConstants.TYPE_A) || (rec.getType() == DNSConstants.TYPE_AAAA))
            {
                hostConflictDetected |= rec.handleResponse(this);
            }
            else
            {
                serviceConflictDetected |= rec.handleResponse(this);
            }

            // notify the listeners
            if (isInformative)
            {
                updateRecord(now, rec);
            }
        }

        if (hostConflictDetected || serviceConflictDetected)
        {
            new Prober().start();
        }
    }

    /**
     * Handle an incoming query. See if we can answer any part of it
     * given our service infos.
     */
    private void handleQuery(DNSIncoming in, InetAddress addr, int port) throws IOException
    {
        // Track known answers
        boolean hostConflictDetected = false;
        boolean serviceConflictDetected = false;
        long expirationTime = System.currentTimeMillis() + DNSConstants.KNOWN_ANSWER_TTL;
        for (Iterator i = in.answers.iterator(); i.hasNext();)
        {
            DNSRecord answer = (DNSRecord) i.next();
            if ((answer.getType() == DNSConstants.TYPE_A) || (answer.getType() == DNSConstants.TYPE_AAAA))
            {
                hostConflictDetected |= answer.handleQuery(this, expirationTime);
            }
            else
            {
                serviceConflictDetected |= answer.handleQuery(this, expirationTime);
            }
        }

        if (plannedAnswer != null)
        {
            plannedAnswer.append(in);
        }
        else
        {
            if (in.isTruncated())
            {
                plannedAnswer = in;
            }

            new Responder(in, addr, port).start();
        }

        if (hostConflictDetected || serviceConflictDetected)
        {
            new Prober().start();
        }
    }

    /**
     * Add an answer to a question. Deal with the case when the
     * outgoing packet overflows
     */
    DNSOutgoing addAnswer(DNSIncoming in, InetAddress addr, int port, DNSOutgoing out, DNSRecord rec) throws IOException
    {
        if (out == null)
        {
            out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
        }
        try
        {
            out.addAnswer(in, rec);
        }
        catch (IOException e)
        {
            out.flags |= DNSConstants.FLAGS_TC;
            out.id = in.id;
            out.finish();
            send(out);

            out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
            out.addAnswer(in, rec);
        }
        return out;
    }


    /**
     * Send an outgoing multicast DNS message.
     */
    private void send(DNSOutgoing out) throws IOException
    {
        out.finish();
        if (!out.isEmpty())
        {
            DatagramPacket packet = new DatagramPacket(out.data, out.off, group, DNSConstants.MDNS_PORT);

            try
            {
                DNSIncoming msg = new DNSIncoming(packet);
                logger.finest("send() JmDNS out:" + msg.print(true));
            }
            catch (IOException e)
            {
                logger.throwing(getClass().toString(), "send(DNSOutgoing) - JmDNS can not parse what it sends!!!", e);
            }
            socket.send(packet);
        }
    }

    /**
     * Listen for multicast packets.
     */
    class SocketListener implements Runnable
    {
        public void run()
        {
            try
            {
                byte buf[] = new byte[DNSConstants.MAX_MSG_ABSOLUTE];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                while (state != DNSState.CANCELED)
                {
                    packet.setLength(buf.length);
                    socket.receive(packet);
                    if (state == DNSState.CANCELED)
                    {
                        break;
                    }
                    try
                    {
                        if (localHost.shouldIgnorePacket(packet))
                        {
                            continue;
                        }

                        DNSIncoming msg = new DNSIncoming(packet);
                        logger.finest("SocketListener.run() JmDNS in:" + msg.print(true));

                        synchronized (ioLock)
                        {
                            if (msg.isQuery())
                            {
                                if (packet.getPort() != DNSConstants.MDNS_PORT)
                                {
                                    handleQuery(msg, packet.getAddress(), packet.getPort());
                                }
                                handleQuery(msg, group, DNSConstants.MDNS_PORT);
                            }
                            else
                            {
                                handleResponse(msg);
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        logger.log(Level.WARNING, "run() exception ", e);
                    }
                }
            }
            catch (IOException e)
            {
                if (state != DNSState.CANCELED)
                {
                    logger.log(Level.WARNING, "run() exception ", e);
                    recover();
                }
            }
        }
    }


    /**
     * Periodicaly removes expired entries from the cache.
     */
    private class RecordReaper extends TimerTask
    {
        public void start()
        {
            timer.schedule(this, DNSConstants.RECORD_REAPER_INTERVAL, DNSConstants.RECORD_REAPER_INTERVAL);
        }

        public void run()
        {
            synchronized (JmDNS.this)
            {
                if (state == DNSState.CANCELED)
                {
                    return;
                }
                logger.finest("run() JmDNS reaping cache");

                // Remove expired answers from the cache
                // -------------------------------------
                // To prevent race conditions, we defensively copy all cache
                // entries into a list.
                List list = new ArrayList();
                synchronized (cache)
                {
                    for (Iterator i = cache.iterator(); i.hasNext();)
                    {
                        for (DNSCache.CacheNode n = (DNSCache.CacheNode) i.next(); n != null; n = n.next())
                        {
                            list.add(n.getValue());
                        }
                    }
                }
                // Now, we remove them.
                long now = System.currentTimeMillis();
                for (Iterator i = list.iterator(); i.hasNext();)
                {
                    DNSRecord c = (DNSRecord) i.next();
                    if (c.isExpired(now))
                    {
                        updateRecord(now, c);
                        cache.remove(c);
                    }
                }
            }
        }
    }


    /**
     * The Prober sends three consecutive probes for all service infos
     * that needs probing as well as for the host name.
     * The state of each service info of the host name is advanced, when a probe has
     * been sent for it.
     * When the prober has run three times, it launches an Announcer.
     * <p/>
     * If a conflict during probes occurs, the affected service infos (and affected
     * host name) are taken away from the prober. This eventually causes the prober
     * tho cancel itself.
     */
    private class Prober extends TimerTask
    {
        /**
         * The state of the prober.
         */
        DNSState taskState = DNSState.PROBING_1;

        public Prober()
        {
            // Associate the host name to this, if it needs probing
            if (state == DNSState.PROBING_1)
            {
                task = this;
            }
            // Associate services to this, if they need probing
            synchronized (JmDNS.this)
            {
                for (Iterator iterator = services.values().iterator(); iterator.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) iterator.next();
                    if (info.getState() == DNSState.PROBING_1)
                    {
                        info.task = this;
                    }
                }
            }
        }


        public void start()
        {
            long now = System.currentTimeMillis();
            if (now - lastThrottleIncrement < DNSConstants.PROBE_THROTTLE_COUNT_INTERVAL)
            {
                throttle++;
            }
            else
            {
                throttle = 1;
            }
            lastThrottleIncrement = now;

            if (state == DNSState.ANNOUNCED && throttle < DNSConstants.PROBE_THROTTLE_COUNT)
            {
                timer.schedule(this, random.nextInt(1 + DNSConstants.PROBE_WAIT_INTERVAL), DNSConstants.PROBE_WAIT_INTERVAL);
            }
            else
            {
                timer.schedule(this, DNSConstants.PROBE_CONFLICT_INTERVAL, DNSConstants.PROBE_CONFLICT_INTERVAL);
            }
        }

        public boolean cancel()
        {
            // Remove association from host name to this
            if (task == this)
            {
                task = null;
            }

            // Remove associations from services to this
            synchronized (JmDNS.this)
            {
                for (Iterator i = services.values().iterator(); i.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) i.next();
                    if (info.task == this)
                    {
                        info.task = null;
                    }
                }
            }

            return super.cancel();
        }

        public void run()
        {
            synchronized (ioLock)
            {
                DNSOutgoing out = null;
                try
                {
                    // send probes for JmDNS itself
                    if (state == taskState && task == this)
                    {
                        if (out == null)
                        {
                            out = new DNSOutgoing(DNSConstants.FLAGS_QR_QUERY);
                        }
                        out.addQuestion(new DNSQuestion(localHost.getName(), DNSConstants.TYPE_ANY, DNSConstants.CLASS_IN));
                        DNSRecord answer = localHost.getDNS4AddressRecord();
                        if (answer != null)
                        {
                            out.addAuthorativeAnswer(answer);
                        }
                        answer = localHost.getDNS6AddressRecord();
                        if (answer != null)
                        {
                            out.addAuthorativeAnswer(answer);
                        }
                        advanceState();
                    }
                    // send probes for services
                    // Defensively copy the services into a local list,
                    // to prevent race conditions with methods registerService
                    // and unregisterService.
                    List list;
                    synchronized (JmDNS.this)
                    {
                        list = new LinkedList(services.values());
                    }
                    for (Iterator i = list.iterator(); i.hasNext();)
                    {
                        ServiceInfo info = (ServiceInfo) i.next();

                        synchronized (info)
                        {
                            if (info.getState() == taskState && info.task == this)
                            {
                                info.advanceState();
                                logger.fine("run() JmDNS probing " + info.getQualifiedName() + " state " + info.getState());
                                if (out == null)
                                {
                                    out = new DNSOutgoing(DNSConstants.FLAGS_QR_QUERY);
                                    out.addQuestion(new DNSQuestion(info.getQualifiedName(), DNSConstants.TYPE_ANY, DNSConstants.CLASS_IN));
                                }
                                out.addAuthorativeAnswer(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.priority, info.weight, info.port, localHost.getName()));
                            }
                        }
                    }
                    if (out != null)
                    {
                        logger.finer("run() JmDNS probing #" + taskState);
                        send(out);
                    }
                    else
                    {
                        // If we have nothing to send, another timer taskState ahead
                        // of us has done the job for us. We can cancel.
                        cancel();
                        return;
                    }
                }
                catch (Throwable e)
                {
                    logger.log(Level.WARNING, "run() exception ", e);
                    recover();
                }

                taskState = taskState.advance();
                if (!taskState.isProbing())
                {
                    cancel();

                    new Announcer().start();
                }
            }
        }

    }

    /**
     * The Announcer sends an accumulated query of all announces, and advances
     * the state of all serviceInfos, for which it has sent an announce.
     * The Announcer also sends announcements and advances the state of JmDNS itself.
     * <p/>
     * When the announcer has run two times, it finishes.
     */
    private class Announcer extends TimerTask
    {
        /**
         * The state of the announcer.
         */
        DNSState taskState = DNSState.ANNOUNCING_1;

        public Announcer()
        {
            // Associate host to this, if it needs announcing
            if (state == DNSState.ANNOUNCING_1)
            {
                task = this;
            }
            // Associate services to this, if they need announcing
            synchronized (JmDNS.this)
            {
                for (Iterator s = services.values().iterator(); s.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) s.next();
                    if (info.getState() == DNSState.ANNOUNCING_1)
                    {
                        info.task = this;
                    }
                }
            }
        }

        public void start()
        {
            timer.schedule(this, DNSConstants.ANNOUNCE_WAIT_INTERVAL, DNSConstants.ANNOUNCE_WAIT_INTERVAL);
        }

        public boolean cancel()
        {
            // Remove association from host to this
            if (task == this)
            {
                task = null;
            }

            // Remove associations from services to this
            synchronized (JmDNS.this)
            {
                for (Iterator i = services.values().iterator(); i.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) i.next();
                    if (info.task == this)
                    {
                        info.task = null;
                    }
                }
            }

            return super.cancel();
        }

        public void run()
        {
            DNSOutgoing out = null;
            try
            {
                // send probes for JmDNS itself
                if (state == taskState)
                {
                    if (out == null)
                    {
                        out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
                    }
                    DNSRecord answer = localHost.getDNS4AddressRecord();
                    if (answer != null)
                    {
                        out.addAnswer(answer, 0);
                    }
                    answer = localHost.getDNS6AddressRecord();
                    if (answer != null)
                    {
                        out.addAnswer(answer, 0);
                    }
                    advanceState();
                }
                // send announces for services
                // Defensively copy the services into a local list,
                // to prevent race conditions with methods registerService
                // and unregisterService.
                List list;
                synchronized (JmDNS.this)
                {
                    list = new ArrayList(services.values());
                }
                for (Iterator i = list.iterator(); i.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) i.next();
                    synchronized (info)
                    {
                        if (info.getState() == taskState && info.task == this)
                        {
                            info.advanceState();
                            logger.finer("run() JmDNS announcing " + info.getQualifiedName() + " state " + info.getState());
                            if (out == null)
                            {
                                out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
                            }
                            out.addAnswer(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.getQualifiedName()), 0);
                            out.addAnswer(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.priority, info.weight, info.port, localHost.getName()), 0);
                            out.addAnswer(new DNSRecord.Text(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.text), 0);
                        }
                    }
                }
                if (out != null)
                {
                    logger.finer("run() JmDNS announcing #" + taskState);
                    send(out);
                }
                else
                {
                    // If we have nothing to send, another timer taskState ahead
                    // of us has done the job for us. We can cancel.
                    cancel();
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }

            taskState = taskState.advance();
            if (!taskState.isAnnouncing())
            {
                cancel();

                new Renewer().start();
            }
        }
    }

    /**
     * The Renewer is there to send renewal announcment when the record expire for ours infos.
     */
    private class Renewer extends TimerTask
    {
        /**
         * The state of the announcer.
         */
        DNSState taskState = DNSState.ANNOUNCED;

        public Renewer()
        {
            // Associate host to this, if it needs renewal
            if (state == DNSState.ANNOUNCED)
            {
                task = this;
            }
            // Associate services to this, if they need renewal
            synchronized (JmDNS.this)
            {
                for (Iterator s = services.values().iterator(); s.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) s.next();
                    if (info.getState() == DNSState.ANNOUNCED)
                    {
                        info.task = this;
                    }
                }
            }
        }

        public void start()
        {
            timer.schedule(this, DNSConstants.ANNOUNCED_RENEWAL_TTL_INTERVAL, DNSConstants.ANNOUNCED_RENEWAL_TTL_INTERVAL);
        }

        public boolean cancel()
        {
            // Remove association from host to this
            if (task == this)
            {
                task = null;
            }

            // Remove associations from services to this
            synchronized (JmDNS.this)
            {
                for (Iterator i = services.values().iterator(); i.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) i.next();
                    if (info.task == this)
                    {
                        info.task = null;
                    }
                }
            }

            return super.cancel();
        }

        public void run()
        {
            DNSOutgoing out = null;
            try
            {
                // send probes for JmDNS itself
                if (state == taskState)
                {
                    if (out == null)
                    {
                        out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
                    }
                    DNSRecord answer = localHost.getDNS4AddressRecord();
                    if (answer != null)
                    {
                        out.addAnswer(answer, 0);
                    }
                    answer = localHost.getDNS6AddressRecord();
                    if (answer != null)
                    {
                        out.addAnswer(answer, 0);
                    }
                    advanceState();
                }
                // send announces for services
                // Defensively copy the services into a local list,
                // to prevent race conditions with methods registerService
                // and unregisterService.
                List list;
                synchronized (JmDNS.this)
                {
                    list = new ArrayList(services.values());
                }
                for (Iterator i = list.iterator(); i.hasNext();)
                {
                    ServiceInfo info = (ServiceInfo) i.next();
                    synchronized (info)
                    {
                        if (info.getState() == taskState && info.task == this)
                        {
                            info.advanceState();
                            logger.finer("run() JmDNS announced " + info.getQualifiedName() + " state " + info.getState());
                            if (out == null)
                            {
                                out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
                            }
                            out.addAnswer(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.getQualifiedName()), 0);
                            out.addAnswer(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.priority, info.weight, info.port, localHost.getName()), 0);
                            out.addAnswer(new DNSRecord.Text(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.text), 0);
                        }
                    }
                }
                if (out != null)
                {
                    logger.finer("run() JmDNS announced");
                    send(out);
                }
                else
                {
                    // If we have nothing to send, another timer taskState ahead
                    // of us has done the job for us. We can cancel.
                    cancel();
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }

            taskState = taskState.advance();
            if (!taskState.isAnnounced())
            {
                cancel();

            }
        }
    }

    /**
     * The Responder sends a single answer for the specified service infos
     * and for the host name.
     */
    private class Responder extends TimerTask
    {
        private DNSIncoming in;
        private InetAddress addr;
        private int port;

        public Responder(DNSIncoming in, InetAddress addr, int port)
        {
            this.in = in;
            this.addr = addr;
            this.port = port;
        }

        public void start()
        {
            // According to draft-cheshire-dnsext-multicastdns.txt
            // chapter "8 Responding":
            // We respond immediately if we know for sure, that we are
            // the only one who can respond to the query.
            // In all other cases, we respond within 20-120 ms.
            //
            // According to draft-cheshire-dnsext-multicastdns.txt
            // chapter "7.2 Multi-Packet Known Answer Suppression":
            // We respond after 20-120 ms if the query is truncated.

            boolean iAmTheOnlyOne = true;
            for (Iterator i = in.questions.iterator(); i.hasNext();)
            {
                DNSEntry entry = (DNSEntry) i.next();
                if (entry instanceof DNSQuestion)
                {
                    DNSQuestion q = (DNSQuestion) entry;
                    logger.finest("start() question=" + q);
                    iAmTheOnlyOne &= (q.type == DNSConstants.TYPE_SRV
                        || q.type == DNSConstants.TYPE_TXT
                        || q.type == DNSConstants.TYPE_A
                        || q.type == DNSConstants.TYPE_AAAA
                        || localHost.getName().equalsIgnoreCase(q.name)
                        || services.containsKey(q.name.toLowerCase()));
                    if (!iAmTheOnlyOne)
                    {
                        break;
                    }
                }
            }
            int delay = (iAmTheOnlyOne && !in.isTruncated()) ? 0 : DNSConstants.RESPONSE_MIN_WAIT_INTERVAL + random.nextInt(DNSConstants.RESPONSE_MAX_WAIT_INTERVAL - DNSConstants.RESPONSE_MIN_WAIT_INTERVAL + 1) - in.elapseSinceArrival();
            if (delay < 0)
            {
                delay = 0;
            }
            logger.finest("start() Responder chosen delay=" + delay);
            timer.schedule(this, delay);
        }

        public void run()
        {
            synchronized (ioLock)
            {
                if (plannedAnswer == in)
                {
                    plannedAnswer = null;
                }

                // We use these sets to prevent duplicate records
                // FIXME - This should be moved into DNSOutgoing
                HashSet questions = new HashSet();
                HashSet answers = new HashSet();


                if (state == DNSState.ANNOUNCED)
                {
                    try
                    {
                        long now = System.currentTimeMillis();
                        long expirationTime = now + 1; //=now+DNSConstants.KNOWN_ANSWER_TTL;
                        boolean isUnicast = (port != DNSConstants.MDNS_PORT);


                        // Answer questions
                        for (Iterator iterator = in.questions.iterator(); iterator.hasNext();)
                        {
                            DNSEntry entry = (DNSEntry) iterator.next();
                            if (entry instanceof DNSQuestion)
                            {
                                DNSQuestion q = (DNSQuestion) entry;

                                // for unicast responses the question must be included
                                if (isUnicast)
                                {
                                    //out.addQuestion(q);
                                    questions.add(q);
                                }

                                int type = q.type;
                                if (type == DNSConstants.TYPE_ANY || type == DNSConstants.TYPE_SRV)
                                { // I ama not sure of why there is a special case here [PJYF Oct 15 2004]
                                    if (localHost.getName().equalsIgnoreCase(q.getName()))
                                    {
                                        // type = DNSConstants.TYPE_A;
                                        DNSRecord answer = localHost.getDNS4AddressRecord();
                                        if (answer != null)
                                        {
                                            answers.add(answer);
                                        }
                                        answer = localHost.getDNS6AddressRecord();
                                        if (answer != null)
                                        {
                                            answers.add(answer);
                                        }
                                        type = DNSConstants.TYPE_IGNORE;
                                    }
                                    else
                                    {
                                        if (serviceTypes.containsKey(q.getName().toLowerCase()))
                                        {
                                            type = DNSConstants.TYPE_PTR;
                                        }
                                    }
                                }

                                switch (type)
                                {
                                    case DNSConstants.TYPE_A:
                                        {
                                            // Answer a query for a domain name
                                            //out = addAnswer( in, addr, port, out, host );
                                            DNSRecord answer = localHost.getDNS4AddressRecord();
                                            if (answer != null)
                                            {
                                                answers.add(answer);
                                            }
                                            break;
                                        }
                                    case DNSConstants.TYPE_AAAA:
                                        {
                                            // Answer a query for a domain name
                                            DNSRecord answer = localHost.getDNS6AddressRecord();
                                            if (answer != null)
                                            {
                                                answers.add(answer);
                                            }
                                            break;
                                        }
                                    case DNSConstants.TYPE_PTR:
                                        {
                                            // Answer a query for services of a given type

                                            // find matching services
                                            for (Iterator serviceIterator = services.values().iterator(); serviceIterator.hasNext();)
                                            {
                                                ServiceInfo info = (ServiceInfo) serviceIterator.next();
                                                if (info.getState() == DNSState.ANNOUNCED)
                                                {
                                                    if (q.name.equalsIgnoreCase(info.type))
                                                    {
                                                        DNSRecord answer = localHost.getDNS4AddressRecord();
                                                        if (answer != null)
                                                        {
                                                            answers.add(answer);
                                                        }
                                                        answer = localHost.getDNS6AddressRecord();
                                                        if (answer != null)
                                                        {
                                                            answers.add(answer);
                                                        }
                                                        answers.add(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.getQualifiedName()));
                                                        answers.add(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN | DNSConstants.CLASS_UNIQUE, DNSConstants.DNS_TTL, info.priority, info.weight, info.port, localHost.getName()));
                                                        answers.add(new DNSRecord.Text(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN | DNSConstants.CLASS_UNIQUE, DNSConstants.DNS_TTL, info.text));
                                                    }
                                                }
                                            }
                                            if (q.name.equalsIgnoreCase("_services._mdns._udp.local."))
                                            {
                                                for (Iterator serviceTypeIterator = serviceTypes.values().iterator(); serviceTypeIterator.hasNext();)
                                                {
                                                    answers.add(new DNSRecord.Pointer("_services._mdns._udp.local.", DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, (String) serviceTypeIterator.next()));
                                                }
                                            }
                                            break;
                                        }
                                    case DNSConstants.TYPE_SRV:
                                    case DNSConstants.TYPE_ANY:
                                    case DNSConstants.TYPE_TXT:
                                        {
                                            ServiceInfo info = (ServiceInfo) services.get(q.name.toLowerCase());
                                            if (info != null && info.getState() == DNSState.ANNOUNCED)
                                            {
                                                DNSRecord answer = localHost.getDNS4AddressRecord();
                                                if (answer != null)
                                                {
                                                    answers.add(answer);
                                                }
                                                answer = localHost.getDNS6AddressRecord();
                                                if (answer != null)
                                                {
                                                    answers.add(answer);
                                                }
                                                answers.add(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.getQualifiedName()));
                                                answers.add(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN | DNSConstants.CLASS_UNIQUE, DNSConstants.DNS_TTL, info.priority, info.weight, info.port, localHost.getName()));
                                                answers.add(new DNSRecord.Text(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN | DNSConstants.CLASS_UNIQUE, DNSConstants.DNS_TTL, info.text));
                                            }
                                            break;
                                        }
                                    default :
                                        {
                                            //System.out.println("JmDNSResponder.unhandled query:"+q);
                                            break;
                                        }
                                }
                            }
                        }


                        // remove known answers, if the ttl is at least half of
                        // the correct value. (See Draft Cheshire chapter 7.1.).
                        for (Iterator i = in.answers.iterator(); i.hasNext();)
                        {
                            DNSRecord knownAnswer = (DNSRecord) i.next();
                            if (knownAnswer.ttl > DNSConstants.DNS_TTL / 2 && answers.remove(knownAnswer))
                            {
                                logger.log(Level.FINER, "JmDNS Responder Known Answer Removed");
                            }
                        }


                        // responde if we have answers
                        if (answers.size() != 0)
                        {
                            logger.finer("run() JmDNS responding");
                            DNSOutgoing out = null;
                            if (isUnicast)
                            {
                                out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA, false);
                            }

                            for (Iterator i = questions.iterator(); i.hasNext();)
                            {
                                out.addQuestion((DNSQuestion) i.next());
                            }
                            for (Iterator i = answers.iterator(); i.hasNext();)
                            {
                                out = addAnswer(in, addr, port, out, (DNSRecord) i.next());
                            }
                            send(out);
                        }
                        cancel();
                    }
                    catch (Throwable e)
                    {
                        logger.log(Level.WARNING, "run() exception ", e);
                        close();
                    }
                }
            }
        }
    }

    /**
     * Helper class to resolve service types.
     * <p/>
     * The TypeResolver queries three times consecutively for service types, and then
     * removes itself from the timer.
     * <p/>
     * The TypeResolver will run only if JmDNS is in state ANNOUNCED.
     */
    private class TypeResolver extends TimerTask
    {
        public void start()
        {
            timer.schedule(this, DNSConstants.QUERY_WAIT_INTERVAL, DNSConstants.QUERY_WAIT_INTERVAL);
        }

        /**
         * Counts the number of queries that were sent.
         */
        int count = 0;

        public void run()
        {
            try
            {
                if (state == DNSState.ANNOUNCED)
                {
                    if (++count < 3)
                    {
                        logger.finer("run() JmDNS querying type");
                        DNSOutgoing out = new DNSOutgoing(DNSConstants.FLAGS_QR_QUERY);
                        out.addQuestion(new DNSQuestion("_services._mdns._udp.local.", DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN));
                        for (Iterator iterator = serviceTypes.values().iterator(); iterator.hasNext();)
                        {
                            out.addAnswer(new DNSRecord.Pointer("_services._mdns._udp.local.", DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, (String) iterator.next()), 0);
                        }
                        send(out);
                    }
                    else
                    {
                        // After three queries, we can quit.
                        cancel();
                    }
                    ;
                }
                else
                {
                    if (state == DNSState.CANCELED)
                    {
                        cancel();
                    }
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }
        }
    }

    /**
     * The ServiceResolver queries three times consecutively for services of
     * a given type, and then removes itself from the timer.
     * <p/>
     * The ServiceResolver will run only if JmDNS is in state ANNOUNCED.
     * REMIND: Prevent having multiple service resolvers for the same type in the
     * timer queue.
     */
    private class ServiceResolver extends TimerTask
    {
        /**
         * Counts the number of queries being sent.
         */
        int count = 0;
        private String type;

        public ServiceResolver(String type)
        {
            this.type = type;
        }

        public void start()
        {
            timer.schedule(this, DNSConstants.QUERY_WAIT_INTERVAL, DNSConstants.QUERY_WAIT_INTERVAL);
        }

        public void run()
        {
            try
            {
                if (state == DNSState.ANNOUNCED)
                {
                    if (count++ < 3)
                    {
                        logger.finer("run() JmDNS querying service");
                        long now = System.currentTimeMillis();
                        DNSOutgoing out = new DNSOutgoing(DNSConstants.FLAGS_QR_QUERY);
                        out.addQuestion(new DNSQuestion(type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN));
                        for (Iterator s = services.values().iterator(); s.hasNext();)
                        {
                            final ServiceInfo info = (ServiceInfo) s.next();
                            try
                            {
                                out.addAnswer(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, DNSConstants.DNS_TTL, info.getQualifiedName()), now);
                            }
                            catch (IOException ee)
                            {
                                break;
                            }
                        }
                        send(out);
                    }
                    else
                    {
                        // After three queries, we can quit.
                        cancel();
                    }
                    ;
                }
                else
                {
                    if (state == DNSState.CANCELED)
                    {
                        cancel();
                    }
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }
        }
    }

    /**
     * The ServiceInfoResolver queries up to three times consecutively for
     * a service info, and then removes itself from the timer.
     * <p/>
     * The ServiceInfoResolver will run only if JmDNS is in state ANNOUNCED.
     * REMIND: Prevent having multiple service resolvers for the same info in the
     * timer queue.
     */
    private class ServiceInfoResolver extends TimerTask
    {
        /**
         * Counts the number of queries being sent.
         */
        int count = 0;
        private ServiceInfo info;

        public ServiceInfoResolver(ServiceInfo info)
        {
            this.info = info;
            info.dns = JmDNS.this;
            addListener(info, new DNSQuestion(info.getQualifiedName(), DNSConstants.TYPE_ANY, DNSConstants.CLASS_IN));
        }

        public void start()
        {
            timer.schedule(this, DNSConstants.QUERY_WAIT_INTERVAL, DNSConstants.QUERY_WAIT_INTERVAL);
        }

        public void run()
        {
            try
            {
                if (state == DNSState.ANNOUNCED)
                {
                    if (count++ < 3 && !info.hasData())
                    {
                        long now = System.currentTimeMillis();
                        DNSOutgoing out = new DNSOutgoing(DNSConstants.FLAGS_QR_QUERY);
                        out.addQuestion(new DNSQuestion(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN));
                        out.addQuestion(new DNSQuestion(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN));
                        if (info.server != null)
                        {
                            out.addQuestion(new DNSQuestion(info.server, DNSConstants.TYPE_A, DNSConstants.CLASS_IN));
                        }
                        out.addAnswer((DNSRecord) cache.get(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN), now);
                        out.addAnswer((DNSRecord) cache.get(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN), now);
                        if (info.server != null)
                        {
                            out.addAnswer((DNSRecord) cache.get(info.server, DNSConstants.TYPE_A, DNSConstants.CLASS_IN), now);
                        }
                        send(out);
                    }
                    else
                    {
                        // After three queries, we can quit.
                        cancel();
                        removeListener(info);
                    }
                    ;
                }
                else
                {
                    if (state == DNSState.CANCELED)
                    {
                        cancel();
                        removeListener(info);
                    }
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }
        }
    }

    /**
     * The Canceler sends two announces with TTL=0 for the specified services.
     */
    private class Canceler extends TimerTask
    {
        /**
         * Counts the number of announces being sent.
         */
        int count = 0;
        /**
         * The services that need cancelling.
         * Note: We have to use a local variable here, because the services
         * that are canceled, are removed immediately from variable JmDNS.services.
         */
        private ServiceInfo[] infos;
        /**
         * We call notifyAll() on the lock object, when we have canceled the
         * service infos.
         * This is used by method JmDNS.unregisterService() and
         * JmDNS.unregisterAllServices, to ensure that the JmDNS
         * socket stays open until the Canceler has canceled all services.
         * <p/>
         * Note: We need this lock, because ServiceInfos do the transition from
         * state ANNOUNCED to state CANCELED before we get here. We could get
         * rid of this lock, if we added a state named CANCELLING to DNSState.
         */
        private Object lock;
        int ttl = 0;

        public Canceler(ServiceInfo info, Object lock)
        {
            this.infos = new ServiceInfo[]{info};
            this.lock = lock;
            addListener(info, new DNSQuestion(info.getQualifiedName(), DNSConstants.TYPE_ANY, DNSConstants.CLASS_IN));
        }

        public Canceler(ServiceInfo[] infos, Object lock)
        {
            this.infos = infos;
            this.lock = lock;
        }

        public Canceler(Collection infos, Object lock)
        {
            this.infos = (ServiceInfo[]) infos.toArray(new ServiceInfo[infos.size()]);
            this.lock = lock;
        }

        public void start()
        {
            timer.schedule(this, 0, DNSConstants.ANNOUNCE_WAIT_INTERVAL);
        }

        public void run()
        {
            try
            {
                if (++count < 3)
                {
                    logger.finer("run() JmDNS canceling service");
                    // announce the service
                    //long now = System.currentTimeMillis();
                    DNSOutgoing out = new DNSOutgoing(DNSConstants.FLAGS_QR_RESPONSE | DNSConstants.FLAGS_AA);
                    for (int i = 0; i < infos.length; i++)
                    {
                        ServiceInfo info = infos[i];
                        out.addAnswer(new DNSRecord.Pointer(info.type, DNSConstants.TYPE_PTR, DNSConstants.CLASS_IN, ttl, info.getQualifiedName()), 0);
                        out.addAnswer(new DNSRecord.Service(info.getQualifiedName(), DNSConstants.TYPE_SRV, DNSConstants.CLASS_IN, ttl, info.priority, info.weight, info.port, localHost.getName()), 0);
                        out.addAnswer(new DNSRecord.Text(info.getQualifiedName(), DNSConstants.TYPE_TXT, DNSConstants.CLASS_IN, ttl, info.text), 0);
                        DNSRecord answer = localHost.getDNS4AddressRecord();
                        if (answer != null)
                        {
                            out.addAnswer(answer, 0);
                        }
                        answer = localHost.getDNS6AddressRecord();
                        if (answer != null)
                        {
                            out.addAnswer(answer, 0);
                        }
                    }
                    send(out);
                }
                else
                {
                    // After three successful announcements, we are finished.
                    synchronized (lock)
                    {
                        closed=true;
                        lock.notifyAll();
                    }
                    cancel();
                }
            }
            catch (Throwable e)
            {
                logger.log(Level.WARNING, "run() exception ", e);
                recover();
            }
        }
    }
    
    // REMIND: Why is this not an anonymous inner class?
    /**
     * Shutdown operations.
     */
    private class Shutdown implements Runnable
    {
        public void run()
        {
            shutdown = null;
            close();
        }
    }

    /**
     * Recover jmdns when there is an error.
     */
    protected void recover()
    {
        logger.finer("recover()");
        // We have an IO error so lets try to recover if anything happens lets close it.
        // This should cover the case of the IP address changing under our feet
        if (DNSState.CANCELED != state)
        {
            synchronized (this)
            { // Synchronize only if we are not already in process to prevent dead locks
                //
                logger.finer("recover() Cleanning up");
                // Stop JmDNS
                state = DNSState.CANCELED; // This protects against recursive calls

                // We need to keep a copy for reregistration
                Collection oldServiceInfos = new ArrayList(services.values());

                // Cancel all services
                unregisterAllServices();
                disposeServiceCollectors();
                //
                // close multicast socket
                closeMulticastSocket();
                //
                cache.clear();
                logger.finer("recover() All is clean");
                //
                // All is clear now start the services
                //
                try
                {
                    openMulticastSocket(localHost);
                    start(oldServiceInfos);
                }
                catch (Exception exception)
                {
                    logger.log(Level.WARNING, "recover() Start services exception ", exception);
                }
                logger.log(Level.WARNING, "recover() We are back!");
            }
        }
    }

    /**
     * Close down jmdns. Release all resources and unregister all services.
     */
    public void close()
    {
        if (state != DNSState.CANCELED)
        {
            synchronized (this)
            { // Synchronize only if we are not already in process to prevent dead locks
                // Stop JmDNS
                state = DNSState.CANCELED; // This protects against recursive calls

                unregisterAllServices();
                disposeServiceCollectors();

                // close socket
                closeMulticastSocket();

                // Stop the timer
                timer.cancel();

                // remove the shutdown hook
                if (shutdown != null)
                {
                    Runtime.getRuntime().removeShutdownHook(shutdown);
                }

            }
        }
    }

    /**
     * List cache entries, for debugging only.
     */
    void print()
    {
        System.out.println("---- cache ----");
        cache.print();
        System.out.println();
    }

    /**
     * List Services and serviceTypes.
     * Debugging Only
     */

    public void printServices()
    {
        System.err.println(toString());
    }

    public String toString()
    {
        StringBuffer aLog = new StringBuffer();
        aLog.append("\t---- Services -----");
        if (services != null)
        {
            for (Iterator k = services.keySet().iterator(); k.hasNext();)
            {
                Object key = k.next();
                aLog.append("\n\t\tService: " + key + ": " + services.get(key));
            }
        }
        aLog.append("\n");
        aLog.append("\t---- Types ----");
        if (serviceTypes != null)
        {
            for (Iterator k = serviceTypes.keySet().iterator(); k.hasNext();)
            {
                Object key = k.next();
                aLog.append("\n\t\tType: " + key + ": " + serviceTypes.get(key));
            }
        }
        aLog.append("\n");
        aLog.append(cache.toString());
        aLog.append("\n");
        aLog.append("\t---- Service Collectors ----");
        if (serviceCollectors != null)
        {
            synchronized (serviceCollectors)
            {
                for (Iterator k = serviceCollectors.keySet().iterator(); k.hasNext();)
                {
                    Object key = k.next();
                    aLog.append("\n\t\tService Collector: " + key + ": " + serviceCollectors.get(key));
                }
                serviceCollectors.clear();
            }
        }
        return aLog.toString();
    }

    /**
     * Returns a list of service infos of the specified type.
     *
     * @param type Service type name, such as <code>_http._tcp.local.</code>.
     * @return An array of service instance names.
     */
    public ServiceInfo[] list(String type)
    {
        // Implementation note: The first time a list for a given type is
        // requested, a ServiceCollector is created which collects service
        // infos. This greatly speeds up the performance of subsequent calls
        // to this method. The caveats are, that 1) the first call to this method
        // for a given type is slow, and 2) we spawn a ServiceCollector
        // instance for each service type which increases network traffic a
        // little.

        ServiceCollector collector;

        boolean newCollectorCreated;
        synchronized (serviceCollectors)
        {
            collector = (ServiceCollector) serviceCollectors.get(type);
            if (collector == null)
            {
                collector = new ServiceCollector(type);
                serviceCollectors.put(type, collector);
                addServiceListener(type, collector);
                newCollectorCreated = true;
            }
            else
            {
                newCollectorCreated = false;
            }
        }

        // After creating a new ServiceCollector, we collect service infos for
        // 200 milliseconds. This should be enough time, to get some service
        // infos from the network.
        if (newCollectorCreated)
        {
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException e)
            {
            }
        }

        return collector.list();
    }

    /**
     * This method disposes all ServiceCollector instances which have been
     * created by calls to method <code>list(type)</code>.
     *
     * @see #list
     */
    private void disposeServiceCollectors()
    {
        logger.finer("disposeServiceCollectors()");
        synchronized (serviceCollectors)
        {
            for (Iterator i = serviceCollectors.values().iterator(); i.hasNext();)
            {
                ServiceCollector collector = (ServiceCollector) i.next();
                removeServiceListener(collector.type, collector);
            }
            serviceCollectors.clear();
        }
    }

    /**
     * Instances of ServiceCollector are used internally to speed up the
     * performance of method <code>list(type)</code>.
     *
     * @see #list
     */
    private static class ServiceCollector implements ServiceListener
    {
        private static Logger logger = Logger.getLogger(ServiceCollector.class.toString());
        /**
         * A set of collected service instance names.
         */
        private Map infos = Collections.synchronizedMap(new HashMap());

        public String type;

        public ServiceCollector(String type)
        {
            this.type = type;
        }

        /**
         * A service has been added.
         */
        public void serviceAdded(ServiceEvent event)
        {
            synchronized (infos)
            {
                event.getDNS().requestServiceInfo(event.getType(), event.getName(), 0);
            }
        }

        /**
         * A service has been removed.
         */
        public void serviceRemoved(ServiceEvent event)
        {
            synchronized (infos)
            {
                infos.remove(event.getName());
            }
        }

        /**
         * A service hase been resolved. Its details are now available in the
         * ServiceInfo record.
         */
        public void serviceResolved(ServiceEvent event)
        {
            synchronized (infos)
            {
                infos.put(event.getName(), event.getInfo());
            }
        }

        /**
         * Returns an array of all service infos which have been collected by this
         * ServiceCollector.
         */
        public ServiceInfo[] list()
        {
            synchronized (infos)
            {
                return (ServiceInfo[]) infos.values().toArray(new ServiceInfo[infos.size()]);
            }
        }

        public String toString()
        {
            StringBuffer aLog = new StringBuffer();
            synchronized (infos)
            {
                for (Iterator k = infos.keySet().iterator(); k.hasNext();)
                {
                    Object key = k.next();
                    aLog.append("\n\t\tService: " + key + ": " + infos.get(key));
                }
            }
            return aLog.toString();
        }
    };
    
    private static String toUnqualifiedName(String type, String qualifiedName)
    {
        if (qualifiedName.endsWith(type))
        {
            return qualifiedName.substring(0, qualifiedName.length() - type.length() - 1);
        }
        else
        {
            return qualifiedName;
        }
    }
}

