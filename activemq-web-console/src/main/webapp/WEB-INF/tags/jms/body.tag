<%@ attribute name="message" type="javax.jms.Message" required="true"  %>
<%@ attribute name="var" type="java.lang.String" required="true"  %>
<%@ tag import="javax.jms.TextMessage" %>
<%@ tag import="javax.jms.ObjectMessage" %>
<%
  Object value = null;
	if (message != null) { 
	  if (message instanceof TextMessage) {
	  		value = ((TextMessage) message).getText();
  		}
  		else if (message instanceof ObjectMessage) {
  			value = ((ObjectMessage) message).getObject();
  	  }
  	}
	request.setAttribute(var, value);
  System.out.println("var: " + var + " is now: " + value);
%>
