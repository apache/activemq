package com.panacya.platform.service.bus.sender;

import javax.ejb.EJBObject;
import java.rmi.RemoteException;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */

public interface Sender extends EJBObject {

    public void sendMessage(String message) throws RemoteException, SenderException;
}
