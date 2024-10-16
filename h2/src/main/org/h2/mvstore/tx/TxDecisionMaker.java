/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.function.Function;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Decision;
import org.h2.mvstore.type.DataType;
import org.h2.value.VersionedValue;

/** 用于 transaction map 的修改
 * Class TxDecisionMaker is a base implementation of MVMap.DecisionMaker
 * to be used for TransactionMap modification.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
class TxDecisionMaker<K,V> extends MVMap.DecisionMaker<VersionedValue<V>> {
    /**
     * Map to decide upon
     */
    private final int            mapId;

    /**
     * Key for the map entry to decide upon
     */
    protected     K              key;

    /**
     * Value for the map entry
     */
    private       V              value;

    /**
     * Transaction we are operating within
     */
    private final Transaction    transaction;

    /**
     * Id for the undo log entry created for this modification
     */
    private       long           undoKey;

    /**
     * Id of the last operation, we decided to
     * {@link org.h2.mvstore.MVMap.Decision#REPEAT}.
     */
    private       long           lastOperationId;

    private       Transaction    blockingTransaction;
    private       MVMap.Decision decision;
    private       V              lastValue;

    TxDecisionMaker(int mapId, Transaction transaction) {
        this.mapId = mapId;
        this.transaction = transaction;
    }

    void initialize(K key, V value) {
        this.key = key; // 行的主键
        this.value = value; // 行
        decision = null;
        reset();
    }

    @Override
    public MVMap.Decision decide(VersionedValue<V> existingValue, VersionedValue<V> providedValue) {
        assert decision == null;
        long id;
        int blockingId;
        // if map does not have that entry yet
        if (existingValue == null ||
                // or entry is a committed one
                (id = existingValue.getOperationId()) == 0 ||
                // or it came from the same transaction
                isThisTransaction(blockingId = TransactionStore.getTransactionId(id))) {
            logAndDecideToPut(existingValue, existingValue == null ? null : existingValue.getCommittedValue());
        } else if (isCommitted(blockingId)) {
            // Condition above means that entry belongs to a committing transaction.
            // We assume that we are looking at the final value for this transaction,
            // and if it's not the case, then it will fail later,
            // because a tree root has definitely been changed.
            V currentValue = existingValue.getCurrentValue();
            logAndDecideToPut(currentValue == null ? null : VersionedValueCommitted.getInstance(currentValue),
                                currentValue);
        } else if (getBlockingTransaction() != null) {
            // this entry comes from a different transaction, and this
            // transaction is not committed yet
            // should wait on blockingTransaction that was determined earlier
            lastValue = existingValue.getCurrentValue();
            decision = MVMap.Decision.ABORT;
        } else if (isRepeatedOperation(id)) {
            // There is no transaction with that id, and we've tried it just
            // before, but map root has not changed (which must be the case if
            // we just missed a closed transaction), therefore we came back here
            // again.
            // Now we assume it's a leftover after unclean shutdown (map update
            // was written but not undo log), and will effectively roll it back
            // (just assume committed value and overwrite).
            V committedValue = existingValue.getCommittedValue();
            logAndDecideToPut(committedValue == null ? null : VersionedValueCommitted.getInstance(committedValue),
                                committedValue);
        } else {
            // transaction has been committed/rolled back and is closed by now, so
            // we can retry immediately and either that entry become committed
            // or we'll hit case above
            decision = MVMap.Decision.REPEAT;
        }
        return decision;
    }

    @Override
    public final void reset() {
        if (decision != MVMap.Decision.REPEAT) {
            lastOperationId = 0;
            if (decision == MVMap.Decision.PUT) {
                // positive decision has been made already and undo record created,
                // but map was updated afterwards and undo record deletion required
                transaction.logUndo();
            }
        }
        blockingTransaction = null;
        decision = null;
        lastValue = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    // always return value (ignores existingValue)
    public <T extends VersionedValue<V>> T selectValue(T existingValue, T providedValue) {
        return (T) VersionedValueUncommitted.getInstance(undoKey, getNewValue(existingValue), lastValue);
    }

    /**
     * Get the new value.
     * This implementation always return the current value (ignores the parameter).
     *
     * @param existingValue the parameter value
     * @return the current value.
     */
    V getNewValue(VersionedValue<V> existingValue) {
        return value;
    }

    /** 创建 undo log
     * Create undo log entry and record for future references
     * {@link org.h2.mvstore.MVMap.Decision#PUT} decision along with last known
     * committed value
     *
     * @param valueToLog previous value to be logged
     * @param lastValue last known committed value
     * @return {@link org.h2.mvstore.MVMap.Decision#PUT}
     */
    MVMap.Decision logAndDecideToPut(VersionedValue<V> valueToLog, V lastValue) {
        undoKey = transaction.log(new Record<>(mapId, key, valueToLog)); // 1.记录 undo log. key 比如是主键 id
        this.lastValue = lastValue;
        return setDecision(MVMap.Decision.PUT); // 2.put 操作
    }

    final MVMap.Decision decideToAbort(V lastValue) {
        this.lastValue = lastValue;
        return setDecision(Decision.ABORT);
    }

    final boolean allowNonRepeatableRead() {
        return transaction.allowNonRepeatableRead();
    }

    final MVMap.Decision getDecision() {
        return decision;
    }

    final Transaction getBlockingTransaction() {
        return blockingTransaction;
    }

    final V getLastValue() {
        return lastValue;
    }

    /**
     * Check whether specified transaction id belongs to "current" transaction
     * (transaction we are acting within).
     *
     * @param transactionId to check
     * @return true it it is "current" transaction's id, false otherwise
     */
    final boolean isThisTransaction(int transactionId) {
        return transactionId == transaction.transactionId;
    }

    /**
     * Determine whether specified id corresponds to a logically committed transaction.
     * In case of pending transaction, reference to actual Transaction object (if any)
     * is preserved for future use.
     *
     * @param transactionId to use
     * @return true if transaction should be considered as committed, false otherwise
     */
    final boolean isCommitted(int transactionId) {
        Transaction blockingTx;
        boolean result;
        TransactionStore store = transaction.store;
        do {
            blockingTx = store.getTransaction(transactionId);
            result = store.committingTransactions.get().get(transactionId);
        } while (blockingTx != store.getTransaction(transactionId));

        if (!result) {
            blockingTransaction = blockingTx;
        }
        return result;
    }

    /**
     * Store operation id provided, but before that, compare it against last stored one.
     * This is to prevent an infinite loop in case of uncommitted "leftover" entry
     * (one without a corresponding undo log entry, most likely as a result of unclean shutdown).
     *
     * @param id
     *            for the operation we decided to
     *            {@link org.h2.mvstore.MVMap.Decision#REPEAT}
     * @return true if the same as last operation id, false otherwise
     */
    final boolean isRepeatedOperation(long id) {
        if (id == lastOperationId) {
            return true;
        }
        lastOperationId = id;
        return false;
    }

    /**
     * Record for future references specified value as a decision that has been made.
     *
     * @param decision made
     * @return argument provided
     */
    final MVMap.Decision setDecision(MVMap.Decision decision) {
        return this.decision = decision;
    }

    @Override
    public final String toString() {
        return "txdm " + transaction.transactionId;
    }



    public static final class PutIfAbsentDecisionMaker<K,V> extends TxDecisionMaker<K,V> {
        private final Function<K, V> oldValueSupplier;

        PutIfAbsentDecisionMaker(int mapId, Transaction transaction, Function<K, V> oldValueSupplier) {
            super(mapId, transaction);
            this.oldValueSupplier = oldValueSupplier; // 提供旧值
        }

        @Override
        public MVMap.Decision decide(VersionedValue<V> existingValue, VersionedValue<V> providedValue) {
            assert getDecision() == null; // 确保当前决策为空
            int blockingId;
            // if map does not have that entry yet
            if (existingValue == null) { // 1.如果 map 中尚无该条目
                V snapshotValue = getValueInSnapshot(); // 1.1.从快照中获取值
                if (snapshotValue != null) { // 如果值存在于快照中但不在当前映射中，说明它已被其他事务移除并提交
                    // value exists in a snapshot but not in current map, therefore
                    // it was removed and committed by another transaction
                    return decideToAbort(snapshotValue); // 进行 abort
                }
                return logAndDecideToPut(null, null); // 1.2.记录 undo log & put 操作
            } else {
                long id = existingValue.getOperationId(); // 获取现有值的操作ID
                if (id == 0 // entry is a committed one
                            // or it came from the same transaction
                        || isThisTransaction(blockingId = TransactionStore.getTransactionId(id))) {
                    if(existingValue.getCurrentValue() != null) {
                        return decideToAbort(existingValue.getCurrentValue()); // 如果当前值不为空，决定中止
                    }
                    if (id == 0) { // 如果ID为0，表示可能是条目被移除的情况，检查快照值
                        V snapshotValue = getValueInSnapshot();
                        if (snapshotValue != null) { // 如果快照值存在，决定中止
                            return decideToAbort(snapshotValue);
                        }
                    }
                    return logAndDecideToPut(existingValue, existingValue.getCommittedValue()); // 记录 undo log 并决定放入已提交的值
                } else if (isCommitted(blockingId)) {
                    // entry belongs to a committing transaction
                    // and therefore will be committed soon
                    if(existingValue.getCurrentValue() != null) {
                        return decideToAbort(existingValue.getCurrentValue());
                    }
                    // even if that commit will result in entry removal
                    // current operation should fail within repeatable read transaction
                    // if initial snapshot carries some value
                    V snapshotValue = getValueInSnapshot();
                    if (snapshotValue != null) {
                        return decideToAbort(snapshotValue);
                    }
                    return logAndDecideToPut(null, null);
                } else if (getBlockingTransaction() != null) {
                    // this entry comes from a different transaction, and this
                    // transaction is not committed yet
                    // should wait on blockingTransaction that was determined
                    // earlier and then try again
                    return decideToAbort(existingValue.getCurrentValue());
                } else if (isRepeatedOperation(id)) {
                    // There is no transaction with that id, and we've tried it
                    // just before, but map root has not changed (which must be
                    // the case if we just missed a closed transaction),
                    // therefore we came back here again.
                    // Now we assume it's a leftover after unclean shutdown (map
                    // update was written but not undo log), and will
                    // effectively roll it back (just assume committed value and
                    // overwrite).
                    V committedValue = existingValue.getCommittedValue();
                    if (committedValue != null) {
                        return decideToAbort(committedValue);
                    }
                    return logAndDecideToPut(null, null);
                } else {
                    // transaction has been committed/rolled back and is closed
                    // by now, so we can retry immediately and either that entry
                    // become committed or we'll hit case above
                    return setDecision(MVMap.Decision.REPEAT);
                }
            }
        }

        private V getValueInSnapshot() {
            return allowNonRepeatableRead() ? null : oldValueSupplier.apply(key); // 是否允许不可重复读(RC 和以下)
        }
    }


    public static class LockDecisionMaker<K,V> extends TxDecisionMaker<K,V> {

        LockDecisionMaker(int mapId, Transaction transaction) {
            super(mapId, transaction);
        }

        @Override
        public MVMap.Decision decide(VersionedValue<V> existingValue, VersionedValue<V> providedValue) {
            MVMap.Decision decision = super.decide(existingValue, providedValue);
            if (existingValue == null) {
                assert decision == MVMap.Decision.PUT;
                decision = setDecision(MVMap.Decision.REMOVE);
            }
            return decision;
        }

        @Override
        V getNewValue(VersionedValue<V> existingValue) {
            return existingValue == null ? null : existingValue.getCurrentValue();
        }
    }

    public static final class RepeatableReadLockDecisionMaker<K,V> extends LockDecisionMaker<K,V> {

        private final DataType<VersionedValue<V>> valueType;

        private final Function<K,V> snapshotValueSupplier;

        RepeatableReadLockDecisionMaker(int mapId, Transaction transaction,
                DataType<VersionedValue<V>> valueType, Function<K,V> snapshotValueSupplier) {
            super(mapId, transaction);
            this.valueType = valueType;
            this.snapshotValueSupplier = snapshotValueSupplier;
        }

        @Override
        Decision logAndDecideToPut(VersionedValue<V> valueToLog, V value) {
            V snapshotValue = snapshotValueSupplier.apply(key);
            if (snapshotValue != null && (valueToLog == null
                    || valueType.compare(VersionedValueCommitted.getInstance(snapshotValue), valueToLog) != 0)) {
                throw DataUtils.newMVStoreException(DataUtils.ERROR_TRANSACTIONS_DEADLOCK, "");
            }
            return super.logAndDecideToPut(valueToLog, value);
        }
    }
}
