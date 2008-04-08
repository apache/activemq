//Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL

package org.apache.activemq.jmdns;

import java.util.EventListener;

/**
 * Listener for service types.
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff, Werner Randelshofer
 */
public interface ServiceTypeListener extends EventListener
{
    /**
     * A new service type was discovered.
     *
     * @param event The service event providing the fully qualified type of
     *              the service.
     */
    void serviceTypeAdded(ServiceEvent event);
}
