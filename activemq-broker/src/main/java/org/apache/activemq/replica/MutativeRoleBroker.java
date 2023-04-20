package org.apache.activemq.replica;

public interface MutativeRoleBroker {

    void stopBeforeRoleChange() throws Exception;

    void startAfterRoleChange() throws Exception;
}
