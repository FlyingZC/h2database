/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.engine.IsolationLevel;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RootReference;
import org.h2.mvstore.type.DataType;
import org.h2.value.VersionedValue;

/**
 * A transaction.
 */
public final class Transaction {

    /**
     * The status of a closed transaction (committed or rolled back).
     */
    public static final int STATUS_CLOSED = 0;

    /**
     * The status of an open transaction.
     */
    public static final int STATUS_OPEN = 1;

    /**
     * The status of a prepared transaction.
     */
    public static final int STATUS_PREPARED = 2;

    /**
     * The status of a transaction that has been logically committed or rather
     * marked as committed, because it might be still listed among prepared,
     * if it was prepared for commit. Undo log entries might still exists for it
     * and not all of it's changes within map's are re-written as committed yet.
     * Nevertheless, those changes should be already viewed by other
     * transactions as committed.
     * This transaction's id can not be re-used until all of the above is completed
     * and transaction is closed.
     * A transactions can be observed in this state when the store was
     * closed while the transaction was not closed yet.
     * When opening a store, such transactions will automatically
     * be processed and closed as committed.
     */
    public static final int STATUS_COMMITTED = 3;

    /**
     * The status of a transaction that currently in a process of rolling back
     * to a savepoint.
     */
    private static final int STATUS_ROLLING_BACK = 4;

    /**
     * The status of a transaction that has been rolled back completely,
     * but undo operations are not finished yet.
     */
    private static final int STATUS_ROLLED_BACK  = 5;

    private static final String[] STATUS_NAMES = {
            "CLOSED", "OPEN", "PREPARED", "COMMITTED", "ROLLING_BACK", "ROLLED_BACK"
    };
    /** 我们在事务中存储的“operation id”中有多少位属于日志id（其余的属于事务id）。
     * How many bits of the "operation id" we store in the transaction belong to the
     * log id (the rest belong to the transaction id).
     */
    static final int LOG_ID_BITS = 40;
    private static final int LOG_ID_BITS1 = LOG_ID_BITS + 1;
    private static final long LOG_ID_LIMIT = 1L << LOG_ID_BITS;
    private static final long LOG_ID_MASK = (1L << LOG_ID_BITS1) - 1;
    private static final int STATUS_BITS = 4;
    private static final int STATUS_MASK = (1 << STATUS_BITS) - 1;


    /**
     * The transaction store.
     */
    final TransactionStore store;

    /**
     * Listener for this transaction's rollback changes.
     */
    final TransactionStore.RollbackListener listener;

    /**
     * The transaction id.
     * More appropriate name for this field would be "slotId"
     */
    final int transactionId;

    /**
     * This is really a transaction identity, because it's not re-used.
     */
    final long sequenceNum;

    /* 事务状态是一个原子复合字段：
     * Transaction state is an atomic composite field:
     * bit  45      : flag whether transaction had rollback(s) 位 45：标记事务是否有回滚
     * bits 44-41   : status 位 44-41：状态位
     * bits 40      : overflow control bit, 1 indicates overflow 40：溢出控制位，1 表示溢出
     * bits 39-0    : log id of the last entry in the undo log map 位 39-0：撤消日志 map 中最后一个条目的日志 ID
     */
    private final AtomicLong statusAndLogId;

    /**
     * Reference to a counter for an earliest store version used by this transaction.
     * Referenced version and all newer ones can not be discarded
     * at least until this transaction ends.
     */
    private MVStore.TxCounter txCounter;

    /**
     * Transaction name.
     */
    private String name;

    /**
     * Indicates whether this transaction was stored in preparedTransactions map
     */
    boolean wasStored;

    /**
     * How long to wait for blocking transaction to commit or rollback.
     */
    int timeoutMillis;

    /**
     * Identification of the owner of this transaction,
     * usually the owner is a database session.
     */
    private final int ownerId;

    /**
     * Blocking transaction, if any
     */
    private volatile Transaction blockingTransaction;

    /**
     * Map on which this transaction is blocked.
     */
    private String blockingMapName;

    /**
     * Key in blockingMap on which this transaction is blocked.
     */
    private Object blockingKey;

    /**
     * Whether other transaction(s) are waiting for this to close.
     */
    private volatile boolean notificationRequested;

    /**
     * RootReferences for undo log snapshots
     */
    private RootReference<Long,Record<?,?>>[] undoLogRootReferences;

    /** 事务 id -> transaction map.
     * Map of transactional maps for this transaction
     */
    private final Map<Integer, TransactionMap<?,?>> transactionMaps = new HashMap<>();

    /**
     * The current isolation level.
     */
    final IsolationLevel isolationLevel;


    Transaction(TransactionStore store, int transactionId, long sequenceNum, int status,
                String name, long logId, int timeoutMillis, int ownerId,
                IsolationLevel isolationLevel, TransactionStore.RollbackListener listener) {
        this.store = store;
        this.transactionId = transactionId;
        this.sequenceNum = sequenceNum;
        this.statusAndLogId = new AtomicLong(composeState(status, logId, false)); // 1. 创建 transaction status & log id
        this.name = name;
        setTimeoutMillis(timeoutMillis);
        this.ownerId = ownerId;
        this.isolationLevel = isolationLevel;
        this.listener = listener;
    }

    public int getId() {
        return transactionId;
    }

    public long getSequenceNum() {
        return sequenceNum;
    }

    public int getStatus() {
        return getStatus(statusAndLogId.get());
    }

    RootReference<Long,Record<?,?>>[] getUndoLogRootReferences() {
        return undoLogRootReferences;
    }

    /** 将事务状态更改为指定值
     * Changes transaction status to a specified value
     * @param status to be set
     * @return transaction state as it was before status change
     */
    private long setStatus(int status) {
        while (true) {
            long currentState = statusAndLogId.get(); // 当前 state & logId
            long logId = getLogId(currentState); // 获取当前 undo log id
            int currentStatus = getStatus(currentState); // 1.获取当前状态
            boolean valid;
            switch (status) { // 2.判断当前状态允许变更为目标状态,然后变更为目标状态
                case STATUS_ROLLING_BACK:
                    valid = currentStatus == STATUS_OPEN;
                    break;
                case STATUS_PREPARED:
                    valid = currentStatus == STATUS_OPEN;
                    break;
                case STATUS_COMMITTED:
                    valid = currentStatus == STATUS_OPEN ||
                            currentStatus == STATUS_PREPARED ||
                            // this case is only possible if called
                            // from endLeftoverTransactions()
                            currentStatus == STATUS_COMMITTED;
                    break;
                case STATUS_ROLLED_BACK:
                    valid = currentStatus == STATUS_OPEN ||
                            currentStatus == STATUS_PREPARED ||
                            currentStatus == STATUS_ROLLING_BACK;
                    break;
                case STATUS_CLOSED:
                    valid = currentStatus == STATUS_COMMITTED ||
                            currentStatus == STATUS_ROLLED_BACK;
                    break;
                case STATUS_OPEN:
                default:
                    valid = false;
                    break;
            }
            if (!valid) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction was illegally transitioned from {0} to {1}",
                        getStatusName(currentStatus), getStatusName(status));
            }
            long newState = composeState(status, logId, hasRollback(currentState)); // 2.新状态(transaction status + undo log id)
            if (statusAndLogId.compareAndSet(currentState, newState)) { // 3.原子变更
                return currentState; // 4.返回当前状态(修改之前的)
            }
        }
    }

    /**
     * Determine if any database changes were made as part of this transaction.
     *
     * @return true if there are changes to commit, false otherwise
     */
    public boolean hasChanges() {
        return hasChanges(statusAndLogId.get());
    }

    public void setName(String name) {
        checkNotClosed();
        this.name = name;
        store.storeTransaction(this);
    }

    public String getName() {
        return name;
    }

    public int getBlockerId() {
        Transaction blocker = this.blockingTransaction;
        return blocker == null ? 0 : blocker.ownerId;
    }

    /** 创建 savepoint, 返回 undo log id
     * Create a new savepoint.
     *
     * @return the savepoint id
     */
    public long setSavepoint() {
        return getLogId();
    }

    /**
     * Returns whether statement dependencies are currently set.
     *
     * @return whether statement dependencies are currently set
     */
    public boolean hasStatementDependencies() {
        return !transactionMaps.isEmpty();
    }

    /**
     * Returns the isolation level of this transaction.
     *
     * @return the isolation level of this transaction
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    boolean isReadCommitted() {
        return isolationLevel == IsolationLevel.READ_COMMITTED;
    }

    /** 判断当前事务是否有 RC 或更低的隔离级别(允许不可重复读)
     * Whether this transaction has isolation level READ_COMMITTED or below.
     * @return true if isolation level is READ_COMMITTED or READ_UNCOMMITTED
     */
    public boolean allowNonRepeatableRead() {
        return isolationLevel.allowNonRepeatableRead();
    }

    /**
     * Mark an entry into a new SQL statement execution within this transaction.
     *
     * @param maps
     *            set of maps used by transaction or statement is about to be executed
     */
    @SuppressWarnings({"unchecked","rawtypes"})
    public void markStatementStart(HashSet<MVMap<Object,VersionedValue<Object>>> maps) {
        markStatementEnd();
        if (txCounter == null && store.store.isVersioningRequired()) { // 需要版本控制
            txCounter = store.store.registerVersionUsage(); // 注册版本使用
        }

        if (maps != null && !maps.isEmpty()) { // 关联的 mvMap 不为空
            // The purpose of the following loop is to get a coherent picture
            // In order to get such a "snapshot", we wait for a moment of silence,
            // when no new transaction were committed / closed.
            BitSet committingTransactions;
            do {
                committingTransactions = store.committingTransactions.get();
                for (MVMap<Object,VersionedValue<Object>> map : maps) { // 为每个 mvMap 创建快照
                    TransactionMap<?,?> txMap = openMapX(map);
                    txMap.setStatementSnapshot(new Snapshot(map.flushAndGetRoot(), committingTransactions));
                }
                if (isReadCommitted()) {
                    undoLogRootReferences = store.collectUndoLogRootReferences();
                }
            } while (committingTransactions != store.committingTransactions.get());
            // Now we have a snapshot, where each map RootReference point to state of the map,
            // undoLogRootReferences captures the state of undo logs
            // and committingTransactions mask tells us which of seemingly uncommitted changes
            // should be considered as committed.
            // Subsequent processing uses this snapshot info only.
            for (MVMap<Object,VersionedValue<Object>> map : maps) {
                TransactionMap<?,?> txMap = openMapX(map);
                txMap.promoteSnapshot();
            }
        }
    }

    /**
     * Mark an exit from SQL statement execution within this transaction.
     */
    public void markStatementEnd() {
        if (allowNonRepeatableRead()) {
            releaseSnapshot();
        }
        for (TransactionMap<?, ?> transactionMap : transactionMaps.values()) {
            transactionMap.setStatementSnapshot(null);
        }
    }

    private void markTransactionEnd() {
        if (!allowNonRepeatableRead()) {
            releaseSnapshot();
        }
    }

    private void releaseSnapshot() {
        transactionMaps.clear();
        undoLogRootReferences = null;
        MVStore.TxCounter counter = txCounter;
        if (counter != null) {
            txCounter = null;
            store.store.deregisterVersionUsage(counter);
        }
    }

    /** 添加 undo log entry
     * Add a log entry.
     *
     * @param logRecord to append
     *
     * @return key for the newly added undo log entry
     */
    long log(Record<?,?> logRecord) {
        long currentState = statusAndLogId.getAndIncrement(); // 1.status&logId 递增
        long logId = getLogId(currentState); // 2.获取 logId
        if (logId >= LOG_ID_LIMIT) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_TRANSACTION_TOO_BIG,
                    "Transaction {0} has too many changes",
                    transactionId);
        }
        int currentStatus = getStatus(currentState); // 3.获取 state
        checkOpen(currentStatus); // 4.校验状态应该是 open
        long undoKey = store.addUndoLogRecord(transactionId, logId, logRecord); // 5.undo log 添加到存储
        return undoKey;
    }

    /**
     * Remove the last log entry.
     */
    void logUndo() {
        long currentState = statusAndLogId.decrementAndGet();
        long logId = getLogId(currentState);
        if (logId >= LOG_ID_LIMIT) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_TRANSACTION_CORRUPT,
                    "Transaction {0} has internal error",
                    transactionId);
        }
        int currentStatus = getStatus(currentState);
        checkOpen(currentStatus);
        store.removeUndoLogRecord(transactionId);
    }

    /**
     * Open a data map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the transaction map
     */
    public <K, V> TransactionMap<K, V> openMap(String name) {
        return openMap(name, null, null);
    }

    /**
     * Open the map to store the data.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param keyType the key data type
     * @param valueType the value data type
     * @return the transaction map
     */
    public <K, V> TransactionMap<K, V> openMap(String name,
                                                DataType<K> keyType,
                                                DataType<V> valueType) {
        MVMap<K, VersionedValue<V>> map = store.openVersionedMap(name, keyType, valueType); // open mv map, value 存的是 versioned value
        return openMapX(map);
    }

    /** 打开/创建给定 map 的事务版本
     * Open the transactional version of the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the base map
     * @return the transactional map
     */
    @SuppressWarnings("unchecked")
    public <K, V> TransactionMap<K,V> openMapX(MVMap<K,VersionedValue<V>> map) {
        checkNotClosed();
        int id = map.getId();
        TransactionMap<K,V> transactionMap = (TransactionMap<K,V>)transactionMaps.get(id); // transaction 缓存
        if (transactionMap == null) {
            transactionMap = new TransactionMap<>(this, map); // 创建 transaction map
            transactionMaps.put(id, transactionMap);
        }
        return transactionMap;
    }

    /**
     * Prepare the transaction. Afterwards, the transaction can only be
     * committed or completely rolled back.
     */
    public void prepare() {
        setStatus(STATUS_PREPARED);
        store.storeTransaction(this);
    }

    /** 提交事务。随后，本次事务结束。
     * Commit the transaction. Afterwards, this transaction is closed.
     */
    public void commit() {
        assert store.openTransactions.get().get(transactionId); // 确认当前事务是打开状态
        markTransactionEnd(); // 1.标记事务结束
        Throwable ex = null;
        boolean hasChanges = false;
        int previousStatus = STATUS_OPEN;
        try {
            long state = setStatus(STATUS_COMMITTED); // 2.设置状态为 已提交
            hasChanges = hasChanges(state); // 3.判断是否有更改(undo log id 不为0)
            previousStatus = getStatus(state); // 获取上一个状态
            if (hasChanges) {
                store.commit(this, previousStatus == STATUS_COMMITTED); // 4.如果有更改, transaction store执行提交操作
            }
        } catch (Throwable e) {
            ex = e;
            throw e;
        } finally {
            if (isActive(previousStatus)) {
                try {
                    store.endTransaction(this, hasChanges);
                } catch (Throwable e) {
                    if (ex == null) {
                        throw e;
                    } else {
                        ex.addSuppressed(e);
                    }
                }
            }
        }
    }

    /**
     * Roll back to the given savepoint. This is only allowed if the
     * transaction is open.
     *
     * @param savepointId the savepoint id
     */
    public void rollbackToSavepoint(long savepointId) {
        long lastState = setStatus(STATUS_ROLLING_BACK);
        long logId = getLogId(lastState);
        boolean success;
        try {
            store.rollbackTo(this, logId, savepointId);
        } finally {
            notifyAllWaitingTransactions();
            long expectedState = composeState(STATUS_ROLLING_BACK, logId, hasRollback(lastState));
            long newState = composeState(STATUS_OPEN, savepointId, true);
            do {
                success = statusAndLogId.compareAndSet(expectedState, newState);
            } while (!success && statusAndLogId.get() == expectedState);
        }
        // this is moved outside of finally block to avert masking original exception, if any
        if (!success) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                    "Transaction {0} concurrently modified while rollback to savepoint was in progress",
                    transactionId);
        }
    }

    /**
     * Roll the transaction back. Afterwards, this transaction is closed.
     */
    public void rollback() {
        markTransactionEnd();
        Throwable ex = null;
        int status = STATUS_OPEN;
        try {
            long lastState = setStatus(STATUS_ROLLED_BACK); // 1.尝试设置事务状态为 ROLLED_BACK，并获取之前的事务状态
            status = getStatus(lastState); // 2.根据之前的事务状态获取当前事务状态
            long logId = getLogId(lastState); // 3.获取事务的日志 ID
            if (logId > 0) {
                store.rollbackTo(this, logId, 0); // 4.如果日志 ID 有效，则执行回滚到指定的日志 ID (从当前事务最大的 log id 回滚到 0)
            }
        } catch (Throwable e) {
            status = getStatus(); // 异常发生时重新获取当前事务状态
            if (isActive(status)) { // 如果事务仍然处于活动状态，则记录异常并抛出
                ex = e;
                throw e;
            }
        } finally {
            try {
                if (isActive(status)) { // 如果事务仍然处于活动状态，则结束事务
                    store.endTransaction(this, true);
                }
            } catch (Throwable e) {
                if (ex == null) { // 如果之前没有捕获异常，则抛出当前异常
                    throw e;
                } else {
                    ex.addSuppressed(e); // 否则，抑制当前异常
                }
            }
        }
    }

    private static boolean isActive(int status) {
        return status != STATUS_CLOSED
            && status != STATUS_COMMITTED
            && status != STATUS_ROLLED_BACK;
    }

    /**
     * Get the list of changes, starting with the latest change, up to the
     * given savepoint (in reverse order than they occurred). The value of
     * the change is the value before the change was applied.
     *
     * @param savepointId the savepoint id, 0 meaning the beginning of the
     *            transaction
     * @return the changes
     */
    public Iterator<TransactionStore.Change> getChanges(long savepointId) {
        return store.getChanges(this, getLogId(), savepointId);
    }

    /**
     * Sets the new lock timeout.
     *
     * @param timeoutMillis the new lock timeout in milliseconds
     */
    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis > 0 ? timeoutMillis : store.timeoutMillis;
    }

    private long getLogId() {
        return getLogId(statusAndLogId.get());
    }

    /**
     * Check whether this transaction is open.
     */
    private void checkOpen(int status) {
        if (status != STATUS_OPEN) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                    "Transaction {0} has status {1}, not OPEN", transactionId, getStatusName(status));
        }
    }

    /**
     * Check whether this transaction is open or prepared.
     */
    private void checkNotClosed() {
        if (getStatus() == STATUS_CLOSED) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_CLOSED, "Transaction {0} is closed", transactionId);
        }
    }

    /**
     * Transition this transaction into a closed state.
     */
    void closeIt() {
        transactionMaps.clear(); // 清理 transaction map
        long lastState = setStatus(STATUS_CLOSED); // 设置状态 closed
        store.store.deregisterVersionUsage(txCounter);
        if((hasChanges(lastState) || hasRollback(lastState))) {
            notifyAllWaitingTransactions();
        }
    }

    private void notifyAllWaitingTransactions() {
        if (notificationRequested) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    /**
     * Make this transaction to wait for the specified transaction to be closed,
     * because both of them try to modify the same map entry.
     *
     * @param toWaitFor transaction to wait for
     * @param mapName name of the map containing blocking entry
     * @param key of the blocking entry
     * @param timeoutMillis timeout in milliseconds, {@code -1} for default
     * @return true if other transaction was closed and this one can proceed, false if timed out
     */
    public boolean waitFor(Transaction toWaitFor, String mapName, Object key, int timeoutMillis) {
        blockingTransaction = toWaitFor;
        blockingMapName = mapName;
        blockingKey = key;
        if (isDeadlocked(toWaitFor)) {
            tryThrowDeadLockException(false);
        }
        boolean result = toWaitFor.waitForThisToEnd(timeoutMillis == -1 ? this.timeoutMillis : timeoutMillis, this);
        blockingMapName = null;
        blockingKey = null;
        blockingTransaction = null;
        return result;
    }

    private boolean isDeadlocked(Transaction toWaitFor) {
        // use transaction sequence No as a tie-breaker
        // the youngest transaction should be selected as a victim
        Transaction youngest = toWaitFor;
        int backstop = store.getMaxTransactionId();
        for(Transaction tx = toWaitFor, nextTx;
            (nextTx = tx.blockingTransaction) != null && tx.getStatus() == Transaction.STATUS_OPEN && backstop > 0;
            tx = nextTx, --backstop) {

            if (nextTx.sequenceNum > youngest.sequenceNum) {
                youngest = nextTx;
            }

            if (nextTx == this) {
                if (youngest == this) {
                    return true;
                }
                Transaction btx = youngest.blockingTransaction;
                if (btx != null) {
                    youngest.setStatus(STATUS_ROLLING_BACK);
                    btx.notifyAllWaitingTransactions();
                    return false;
                }
            }
        }
        return false;
    }

    private void tryThrowDeadLockException(boolean throwIt) {
        BitSet visited = new BitSet();
        StringBuilder details = new StringBuilder(
                String.format("Transaction %d has been chosen as a deadlock victim. Details:%n", transactionId));
        for (Transaction tx = this, nextTx;
                !visited.get(tx.transactionId) &&  (nextTx = tx.blockingTransaction) != null; tx = nextTx) {
            visited.set(tx.transactionId);
            details.append(String.format(
                    "Transaction %d attempts to update map <%s> entry with key <%s> modified by transaction %s%n",
                    tx.transactionId, tx.blockingMapName, tx.blockingKey, tx.blockingTransaction));
            if (nextTx == this) {
                throwIt = true;
            }
        }
        if (throwIt) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_TRANSACTIONS_DEADLOCK, "{0}", details.toString());
        }
    }

    private synchronized boolean waitForThisToEnd(int millis, Transaction waiter) {
        long time = System.nanoTime();
        notificationRequested = true;
        long state;
        int status;
        while ((status = getStatus(state = statusAndLogId.get())) != STATUS_CLOSED
                && status != STATUS_ROLLED_BACK && !hasRollback(state)) {
            if (waiter.getStatus() != STATUS_OPEN) {
                waiter.tryThrowDeadLockException(true);
            }
            int remaining = millis - (int) ((System.nanoTime() - time) / 1_000_000L);
            if (remaining <= 0) {
                return false;
            }
            try {
                wait(remaining);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return true;
    }

    /**
     * Remove the map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    public <K, V> void removeMap(TransactionMap<K, V> map) {
        store.removeMap(map);
    }

    @Override
    public String toString() {
        return transactionId + "(" + sequenceNum + ") " + stateToString();
    }

    private String stateToString() {
        return stateToString(statusAndLogId.get());
    }

    private static String stateToString(long state) {
        return getStatusName(getStatus(state)) + (hasRollback(state) ? "<" : "") + " " + getLogId(state);
    }


    private static int getStatus(long state) {
        return (int)(state >>> LOG_ID_BITS1) & STATUS_MASK;
    }

    private static long getLogId(long state) {
        return state & LOG_ID_MASK;
    }

    private static boolean hasRollback(long state) {
        return (state & (1L << (STATUS_BITS + LOG_ID_BITS1))) != 0;
    }

    private static boolean hasChanges(long state) {
        return getLogId(state) != 0;
    }

    private static long composeState(int status, long logId, boolean hasRollback) { // 组合状态值
        assert logId < LOG_ID_LIMIT : logId;
        assert (status & ~STATUS_MASK) == 0 : status;

        if (hasRollback) {
            status |= 1 << STATUS_BITS; // 如果有回滚操作，则在状态码中设置回滚标志位
        }
        return ((long)status << LOG_ID_BITS1) | logId;  // 返回组合后的状态值，状态码左移后与日志ID进行按位或操作
    }

    private static String getStatusName(int status) {
        return status >= 0 && status < STATUS_NAMES.length ? STATUS_NAMES[status] : "UNKNOWN_STATUS_" + status;
    }
}
