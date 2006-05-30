<%@ attribute name="name" type="java.lang.String" required="true"  %>
<%@ attribute name="value" type="java.lang.String" required="true"  %>
<%@ attribute name="label" type="java.lang.String" required="true"  %>
<option ${param[name] == value ? 'selected' : ''} value="${value}">${label}</option>
