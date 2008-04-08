//Copyright 2003-2005 Arthur van Hoff Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL


package org.apache.activemq.jmdns;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * A table of DNS entries. This is a hash table which
 * can handle multiple entries with the same name.
 * <p/>
 * Storing multiple entries with the same name is implemented using a
 * linked list of <code>CacheNode</code>'s.
 * <p/>
 * The current implementation of the API of DNSCache does expose the
 * cache nodes to clients. Clients must explicitly deal with the nodes
 * when iterating over entries in the cache. Here's how to iterate over
 * all entries in the cache:
 * <pre>
 * for (Iterator i=dnscache.iterator(); i.hasNext(); ) {
 *    for (DNSCache.CacheNode n = (DNSCache.CacheNode) i.next(); n != null; n.next()) {
 *       DNSEntry entry = n.getValue();
 *       ...do something with entry...
 *    }
 * }
 * </pre>
 * <p/>
 * And here's how to iterate over all entries having a given name:
 * <pre>
 * for (DNSCache.CacheNode n = (DNSCache.CacheNode) dnscache.find(name); n != null; n.next()) {
 *     DNSEntry entry = n.getValue();
 *     ...do something with entry...
 * }
 * </pre>
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff, Werner Randelshofer, Rick Blair
 */
class DNSCache
{
    private static Logger logger = Logger.getLogger(DNSCache.class.toString());
    // Implementation note:
    // We might completely hide the existence of CacheNode's in a future version
    // of DNSCache. But this will require to implement two (inner) classes for
    // the  iterators that will be returned by method <code>iterator()</code> and
    // method <code>find(name)</code>.
    // Since DNSCache is not a public class, it does not seem worth the effort
    // to clean its API up that much.

    // [PJYF Oct 15 2004] This should implements Collections that would be amuch cleaner implementation
    
    /**
     * The number of DNSEntry's in the cache.
     */
    private int size;

    /**
     * The hashtable used internally to store the entries of the cache.
     * Keys are instances of String. The String contains an unqualified service
     * name.
     * Values are linked lists of CacheNode instances.
     */
    private HashMap hashtable;

    /**
     * Cache nodes are used to implement storage of multiple DNSEntry's of the
     * same name in the cache.
     */
    public static class CacheNode
    {
        private static Logger logger = Logger.getLogger(CacheNode.class.toString());
        private DNSEntry value;
        private CacheNode next;

        public CacheNode(DNSEntry value)
        {
            this.value = value;
        }

        public CacheNode next()
        {
            return next;
        }

        public DNSEntry getValue()
        {
            return value;
        }
    }


    /**
     * Create a table with a given initial size.
     */
    public DNSCache(final int size)
    {
        hashtable = new HashMap(size);
    }

    /**
     * Clears the cache.
     */
    public synchronized void clear()
    {
        hashtable.clear();
        size = 0;
    }

    /**
     * Adds an entry to the table.
     */
    public synchronized void add(final DNSEntry entry)
    {
        //logger.log("DNSCache.add("+entry.getName()+")");
        CacheNode newValue = new CacheNode(entry);
        CacheNode node = (CacheNode) hashtable.get(entry.getName());
        if (node == null)
        {
            hashtable.put(entry.getName(), newValue);
        }
        else
        {
            newValue.next = node.next;
            node.next = newValue;
        }
        size++;
    }

    /**
     * Remove a specific entry from the table. Returns true if the
     * entry was found.
     */
    public synchronized boolean remove(DNSEntry entry)
    {
        CacheNode node = (CacheNode) hashtable.get(entry.getName());
        if (node != null)
        {
            if (node.value == entry)
            {
                if (node.next == null)
                {
                    hashtable.remove(entry.getName());
                }
                else
                {
                    hashtable.put(entry.getName(), node.next);
                }
                size--;
                return true;
            }

            CacheNode previous = node;
            node = node.next;
            while (node != null)
            {
                if (node.value == entry)
                {
                    previous.next = node.next;
                    size--;
                    return true;
                }
                previous = node;
                node = node.next;
            }
            ;
        }
        return false;
    }

    /**
     * Get a matching DNS entry from the table (using equals).
     * Returns the entry that was found.
     */
    public synchronized DNSEntry get(DNSEntry entry)
    {
        for (CacheNode node = find(entry.getName()); node != null; node = node.next)
        {
            if (node.value.equals(entry))
            {
                return node.value;
            }
        }
        return null;
    }

    /**
     * Get a matching DNS entry from the table.
     */
    public synchronized DNSEntry get(String name, int type, int clazz)
    {
        for (CacheNode node = find(name); node != null; node = node.next)
        {
            if (node.value.type == type && node.value.clazz == clazz)
            {
                return node.value;
            }
        }
        return null;
    }

    /**
     * Iterates over all cache nodes.
     * The iterator returns instances of DNSCache.CacheNode.
     * Each instance returned is the first node of a linked list.
     * To retrieve all entries, one must iterate over this linked list. See
     * code snippets in the header of the class.
     */
    public Iterator iterator()
    {
        return Collections.unmodifiableCollection(hashtable.values()).iterator();
    }

    /**
     * Iterate only over items with matching name.
     * Returns an instance of DNSCache.CacheNode or null.
     * If an instance is returned, it is the first node of a linked list.
     * To retrieve all entries, one must iterate over this linked list.
     */
    public synchronized CacheNode find(String name)
    {
        return (CacheNode) hashtable.get(name);
    }

    /**
     * List all entries for debugging.
     */
    public synchronized void print()
    {
        for (Iterator i = iterator(); i.hasNext();)
        {
            for (CacheNode n = (CacheNode) i.next(); n != null; n = n.next)
            {
                System.out.println(n.value);
            }
        }
    }

    public synchronized String toString()
    {
        StringBuffer aLog = new StringBuffer();
        aLog.append("\t---- cache ----");
        for (Iterator i = iterator(); i.hasNext();)
        {
            for (CacheNode n = (CacheNode) i.next(); n != null; n = n.next)
            {
                aLog.append("\n\t\t" + n.value);
            }
        }
        return aLog.toString();
    }

}
