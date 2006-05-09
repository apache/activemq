<%@ attribute name="name" type="java.lang.String" required="true"  %>
 <input type="checkbox" name="${name}" value="true" ${param[name] ? 'checked' : ''}/>
