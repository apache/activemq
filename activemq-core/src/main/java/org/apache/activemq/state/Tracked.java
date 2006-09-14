/**
 * 
 */
package org.apache.activemq.state;

import org.apache.activemq.command.Response;

public class Tracked extends Response {
	
	private Runnable runnable;
	
	public Tracked(Runnable runnable) {
		this.runnable = runnable;
	}
	
	public void onResponses() {
		if( runnable != null ) {
			runnable.run();
			runnable=null;
		}
	}
	
	public boolean isWaitingForResponse() {
		return runnable!=null;
	}
	
}