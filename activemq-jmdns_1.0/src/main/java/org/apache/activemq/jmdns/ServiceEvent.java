///Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL

package org.apache.activemq.jmdns;

import java.util.EventObject;
import java.util.logging.Logger;

/**
 * ServiceEvent.
 *
 * @author Werner Randelshofer, Rick Blair
 * @version %I%, %G%
 */
public class ServiceEvent extends EventObject
{
    private static Logger logger = Logger.getLogger(ServiceEvent.class.toString());
    /**
     * The type name of the service.
     */
    private String type;
    /**
     * The instance name of the service. Or null, if the event was
     * fired to a service type listener.
     */
    private String name;
    /**
     * The service info record, or null if the service could be be resolved.
     * This is also null, if the event was fired to a service type listener.
     */
    private ServiceInfo info;

    /**
     * Creates a new instance.
     *
     * @param source the JmDNS instance which originated the event.
     * @param type   the type name of the service.
     * @param name   the instance name of the service.
     * @param info   the service info record, or null if the service could be be resolved.
     */
    public ServiceEvent(JmDNS source, String type, String name, ServiceInfo info)
    {
        super(source);
        this.type = type;
        this.name = name;
        this.info = info;
    }

    /**
     * Returns the JmDNS instance which originated the event.
     */
    public JmDNS getDNS()
    {
        return (JmDNS) getSource();
    }

    /**
     * Returns the fully qualified type of the service.
     */
    public String getType()
    {
        return type;
    }

    /**
     * Returns the instance name of the service.
     * Always returns null, if the event is sent to a service type listener.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Returns the service info record, or null if the service could not be
     * resolved.
     * Always returns null, if the event is sent to a service type listener.
     */
    public ServiceInfo getInfo()
    {
        return info;
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("<" + getClass().getName() + "> ");
        buf.append(super.toString());
        buf.append(" name ");
        buf.append(getName());
        buf.append(" type ");
        buf.append(getType());
        buf.append(" info ");
        buf.append(getInfo());
        return buf.toString();
    }

}
