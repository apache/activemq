package com.panacya.platform.service.bus.sender;

import javax.ejb.CreateException;
import javax.ejb.EJBHome;
import java.rmi.RemoteException;


/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */

public interface SenderHome extends EJBHome {

    com.panacya.platform.service.bus.sender.Sender create() throws RemoteException, CreateException;
}
