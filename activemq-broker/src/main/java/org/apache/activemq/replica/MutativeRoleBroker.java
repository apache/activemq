package org.apache.activemq.replica;

public interface MutativeRoleBroker {

    void initializeRoleChangeCallBack(ActionListenerCallback actionListenerCallback);

    void stopBeforeRoleChange(boolean force) throws Exception;

    void startAfterRoleChange() throws Exception;

}
