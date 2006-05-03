<%@ attribute name="message" type="javax.jms.Message" required="true"  %>
<%
	if (message != null) { 
	  if (message.getJMSDeliveryMode() == javax.jms.DeliveryMode.PERSISTENT) {
  		  	  out.println("Persistent");
  		}
  		else {
		  	  out.println("Non Persistent");
  	  }
  	}
%>
