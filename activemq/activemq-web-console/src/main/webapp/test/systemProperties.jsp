<html>
<head>
<title>Test Pages</title>
</head>
<body>

<h2>Test Pages</h2>

These pages are used to test out the environment and web framework.

<table class="sortable autostripe">
<thead>
<tr>
<th>System Property</th>
<th>Value</th>
</tr>
</thead>
<tbody>
    
<%
    for (java.util.Iterator iter = System.getProperties().entrySet().iterator(); iter.hasNext(); ) {
        request.setAttribute("entry", iter.next());
%>    
<tr> 
  <td class="label">${entry.key}</td>
  <td>${entry.value}</td>
</tr>
<%
}
%>
</tbody>
</table>


</body>
</html>
	