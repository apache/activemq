<%@ attribute name="message" type="javax.jms.Message" required="true"  %>
<%
  System.out.println("Got message: " + message);
	if (message != null) { 
	  if (message.getJMSDeliveryMode() == javax.jms.DeliveryMode.PERSISTENT) {
  		  	  out.println("Persistent");
  		}
  		else {
		  	  out.println("Non Persistent");
  	  }
  	}
%>
