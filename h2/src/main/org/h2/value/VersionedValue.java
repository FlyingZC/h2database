/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/** versioned value.如果当前值未提交，则它包含当前值和最新提交的值。 此外，对于未提交的值，它还包含 operationId - transactionId 和 logId 的组合
 * A versioned value (possibly null).
 * It contains current value and latest committed value if current one is uncommitted.
 * Also for uncommitted values it contains operationId - a combination of
 * transactionId and logId.
 */
public class VersionedValue<T> {

    protected VersionedValue() {}

    public boolean isCommitted() {
        return true;
    }

    public long getOperationId() {
        return 0L;
    }

    @SuppressWarnings("unchecked")
    public T getCurrentValue() {
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public T getCommittedValue() {
        return (T)this;
    }

}
