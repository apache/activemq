<%@ attribute name="var" type="java.lang.String" required="true"  %>
<%@ attribute name="items" type="java.util.Map" required="true"  %>
<%@ tag import="java.util.Iterator" %>
<%
  Iterator iter = items.entrySet().iterator();
  while (iter.hasNext()) {
  		 request.setAttribute(var, iter.next());
%>
<jsp:doBody/>
<%
	}
%>       
    