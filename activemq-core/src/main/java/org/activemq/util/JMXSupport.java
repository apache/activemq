package org.activemq.util;

public class JMXSupport {
    static public String encodeObjectNamePart(String part) {
        String answer = part.replaceAll("[\\:\\,\\'\\\"]", "_");
        answer = answer.replaceAll("\\?", "&qe;");
        answer = answer.replaceAll("=", "&amp;");
        return answer;
        
    }
}
