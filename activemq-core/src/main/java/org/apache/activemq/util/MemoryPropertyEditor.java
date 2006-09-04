package org.apache.activemq.util;

import java.beans.PropertyEditorSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MemoryPropertyEditor extends PropertyEditorSupport {
	public void setAsText(String text) throws IllegalArgumentException {

		Pattern p = Pattern.compile("^\\s*(\\d+)\\s*(b)?\\s*$",Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(text);
		if (m.matches()) {
			setValue(new Long(Long.parseLong(m.group(1))));
			return;
		}

		p = Pattern.compile("^\\s*(\\d+)\\s*k(b)?\\s*$",Pattern.CASE_INSENSITIVE);
		m = p.matcher(text);
		if (m.matches()) {
			setValue(new Long(Long.parseLong(m.group(1)) * 1024));
			return;
		}

		p = Pattern.compile("^\\s*(\\d+)\\s*m(b)?\\s*$", Pattern.CASE_INSENSITIVE);
		m = p.matcher(text);
		if (m.matches()) {
			setValue(new Long(Long.parseLong(m.group(1)) * 1024 * 1024 ));
			return;
		}

		p = Pattern.compile("^\\s*(\\d+)\\s*g(b)?\\s*$", Pattern.CASE_INSENSITIVE);
		m = p.matcher(text);
		if (m.matches()) {
			setValue(new Long(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024 ));
			return;
		}

		throw new IllegalArgumentException(
				"Could convert not to a memory size: " + text);
	}

	public String getAsText() {
		Long value = (Long) getValue();
		return (value != null ? value.toString() : "");
	}

}
