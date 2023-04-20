package org.apache.activemq.replica.jmx;

import org.apache.activemq.broker.BrokerService;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class ReplicationJmxHelper {

    private ReplicationJmxHelper() {
    }

    public static ObjectName createJmxName(BrokerService brokerService) {
        try {
            String objectNameStr = brokerService.getBrokerObjectName().toString();

            objectNameStr += "," + "service=Plugins";
            objectNameStr += "," + "instanceName=ReplicationPlugin";

            return new ObjectName(objectNameStr);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Failed to create JMX view for ReplicationPlugin", e);
        }
    }
}