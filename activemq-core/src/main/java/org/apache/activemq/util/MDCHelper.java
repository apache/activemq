package org.apache.activemq.util;

import org.slf4j.MDC;

import java.util.Hashtable;
import java.util.Map;

/**
 *  Helper class as MDC Log4J adapter doesn't behave well with null values
 */
public class MDCHelper {

    public static Map getCopyOfContextMap() {
        Map map = MDC.getCopyOfContextMap();
        if (map == null) {
            map = new Hashtable();
        }
        return map;
    }

    public static void setContextMap(Map map) {
        if (map == null) {
            map = new Hashtable();
        }
        MDC.setContextMap(map);
    }

}
