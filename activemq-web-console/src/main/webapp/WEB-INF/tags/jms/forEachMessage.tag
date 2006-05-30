<%@ attribute name="var" type="java.lang.String" required="true"  %>
<%@ attribute name="queueBrowser" type="javax.jms.QueueBrowser" required="true"  %>
<%@ tag import="java.util.Enumeration" %>
<%@ tag import="javax.jms.Message" %>
<%

	Enumeration iter = queueBrowser.getEnumeration();
	while (iter.hasMoreElements()) {
	    Message message = (Message) iter.nextElement();
	    if (message != null) {
	    		request.setAttribute(var, message);
%>
<jsp:doBody/>
<%
			}
	}
%>       
    