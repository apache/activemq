package org.activemq.openwire;

import org.activemq.command.BrokerId;
import org.activemq.command.BrokerInfo;

public class BrokerInfoData extends DataFileGenerator {

    protected Object createObject() {
        BrokerInfo rc = new BrokerInfo();
        rc.setResponseRequired(false);
        rc.setBrokerName("localhost");
        rc.setBrokerURL("tcp://localhost:61616");
        rc.setBrokerId(new BrokerId("ID:1289012830123"));
        rc.setCommandId((short) 12);
        rc.setResponseRequired(false);
        return rc;
    }

}
