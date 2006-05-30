<%@ attribute name="name" type="java.lang.String" required="true"  %>
<%@ attribute name="defaultValue" type="java.lang.String" required="false"  %>
<%
    String value = request.getParameter(name);
    if (value == null || value.trim().length() == 0) {
    		value = defaultValue;
		}
		if (value == null) {
			value = "";
		}
%>
<input type="text" name="${name}" value="<%= value %>"/>
