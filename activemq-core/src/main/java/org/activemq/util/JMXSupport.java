package org.activemq.util;

import javax.management.ObjectName;

public class JMXSupport {
    static public String encodeObjectNamePart(String part) {
        return ObjectName.quote(part);
        /*
        String answer = part.replaceAll("[\\:\\,\\'\\\"]", "_");
        answer = answer.replaceAll("\\?", "&qe;");
        answer = answer.replaceAll("=", "&amp;");
        return answer;
        */
        
    }
}
