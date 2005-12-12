package org.activemq.openwire;

import org.activemq.command.WireFormatInfo;

public class WireFormatInfoData extends DataFileGenerator {

    protected Object createObject() {
        WireFormatInfo rc = new WireFormatInfo();
        rc.setResponseRequired(false);
        rc.setVersion(1);
        return rc;
    }

}
