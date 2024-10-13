/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Class RootReference is an immutable structure to represent state of the MVMap as a whole
 * (not related to a particular B-Tree node).
 * Single structure would allow for non-blocking atomic state change.
 * The most important part of it is a reference to the root node.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RootReference<K,V> {

    /**
     * The root page.
     */
    public final Page<K,V> root;
    /** 写版本号
     * The version used for writing.
     */
    public final long version;
    /** 加锁数量
     * Counter of reentrant locks.
     */
    private final byte holdCount;
    /** 持有锁的 线程 id
     * Lock owner thread id.
     */
    private final long ownerId;
    /** 上一个版本的 root reference.
     * Reference to the previous root in the chain.
     * That is the last root of the previous version, which had any data changes.
     * Versions without any data changes are dropped from the chain, as it built.
     */
    volatile RootReference<K,V> previous;
    /** 成功更新 root 的计数器。
     * Counter for successful root updates.
     */
    final long updateCounter;
    /**
     * Counter for attempted root updates.
     */
    final long updateAttemptCounter;
    /** 追加缓冲区占用部分的大小
     * Size of the occupied part of the append buffer.
     */
    private final byte appendCounter;


    // This one is used to set root initially and for r/o snapshots
    RootReference(Page<K,V> root, long version) { // btree root 节点引用
        this.root = root; // root page
        this.version = version; // 根据 last chunk version 计算得到的当前 root page 的 version
        this.previous = null;
        this.updateCounter = 1;
        this.updateAttemptCounter = 1;
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = 0;
    }

    private RootReference(RootReference<K,V> r, Page<K,V> root, long updateAttemptCounter) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + updateAttemptCounter;
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = r.appendCounter;
    }

    // This one is used for locking
    private RootReference(RootReference<K,V> r, int attempt) {
        this.root = r.root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        assert r.holdCount == 0 || r.ownerId == Thread.currentThread().getId() //
                : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount + 1);
        this.ownerId = Thread.currentThread().getId();
        this.appendCounter = r.appendCounter;
    }

    // This one is used for unlocking
    private RootReference(RootReference<K,V> r, Page<K,V> root, boolean keepLocked, int appendCounter) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter;
        this.updateAttemptCounter = r.updateAttemptCounter;
        assert r.holdCount > 0 && r.ownerId == Thread.currentThread().getId() //
                : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount - (keepLocked ? 0 : 1));
        this.ownerId = this.holdCount == 0 ? 0 : Thread.currentThread().getId();
        this.appendCounter = (byte) appendCounter;
    }

    // This one is used for version change
    private RootReference(RootReference<K,V> r, long version, int attempt) {
        RootReference<K,V> previous = r; // 保存上一个版本的引用
        RootReference<K,V> tmp;
        while ((tmp = previous.previous) != null && tmp.root == r.root) {
            previous = tmp;
        }
        this.root = r.root; // 设置当前 reference 的 root
        this.version = version;
        this.previous = previous; // 上一个版本的 root reference
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.holdCount = r.holdCount == 0 ? 0 : (byte)(r.holdCount - 1);
        this.ownerId = this.holdCount == 0 ? 0 : r.ownerId;
        assert r.appendCounter == 0;
        this.appendCounter = 0;
    }

    /**
     * Try to unlock.
     *
     * @param newRootPage the new root page
     * @param attemptCounter the number of attempts so far
     * @return the new, unlocked, root reference, or null if not successful
     */
    RootReference<K,V> updateRootPage(Page<K,V> newRootPage, long attemptCounter) {
        return isFree() ? tryUpdate(new RootReference<>(this, newRootPage, attemptCounter)) : null;
    }

    /**
     * Try to lock.
     *
     * @param attemptCounter the number of attempts so far
     * @return the new, locked, root reference, or null if not successful
     */
    RootReference<K,V> tryLock(int attemptCounter) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, attemptCounter)) : null;
    }

    /** 尝试解锁，如果成功则更新版本
     * Try to unlock, and if successful update the version
     *
     * @param version the version
     * @param attempt the number of attempts so far
     * @return the new, unlocked and updated, root reference, or null if not successful
     */
    RootReference<K,V> tryUnlockAndUpdateVersion(long version, int attempt) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, version, attempt)) : null; // 如果当前线程可以更新.则创建 root reference 并原子更新
    }

    /**
     * Update the page, possibly keeping it locked.
     *
     * @param page the page
     * @param keepLocked whether to keep it locked
     * @param appendCounter number of items in append buffer
     * @return the new root reference, or null if not successful
     */
    RootReference<K,V> updatePageAndLockedStatus(Page<K,V> page, boolean keepLocked, int appendCounter) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, page, keepLocked, appendCounter)) : null;
    }

    /**
     * Removed old versions that are not longer used.
     *
     * @param oldestVersionToKeep the oldest version that needs to be retained
     */
    void removeUnusedOldVersions(long oldestVersionToKeep) {
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for(RootReference<K,V> rootRef = this; rootRef != null; rootRef = rootRef.previous) { // 遍历 root reference 的前驱节点
            if (rootRef.version < oldestVersionToKeep) { // 当前根节点的版本小于需要保留的最旧版本号时，移除该节点及其之前的所有版本
                RootReference<K,V> previous;
                assert (previous = rootRef.previous) == null || previous.getAppendCounter() == 0 //
                        : oldestVersionToKeep + " " + rootRef.previous;
                rootRef.previous = null;
            }
        }
    }

    boolean isLocked() {
        return holdCount != 0;
    }

    private boolean isFree() {
        return holdCount == 0; // 加锁数量为0
    }


    private boolean canUpdate() { // 判断当前线程是否可以更新 root reference
        return isFree() || ownerId == Thread.currentThread().getId(); // 没加锁 or 锁被当前线程持有
    }

    public boolean isLockedByCurrentThread() { // 判断当前线程是否持有锁
        return holdCount != 0 && ownerId == Thread.currentThread().getId(); // 锁 owner 是当前线程
    }

    private RootReference<K,V> tryUpdate(RootReference<K,V> updatedRootReference) {
        assert canUpdate();
        return root.map.compareAndSetRoot(this, updatedRootReference) ? updatedRootReference : null; // 原子更新 根节点->mvMap->rootReference
    }

    long getVersion() {
        RootReference<K,V> prev = previous;
        return prev == null || prev.root != root ||
                prev.appendCounter != appendCounter ?
                    version : prev.getVersion();
    }

    /**
     * Does the root have changes since the specified version?
     *
     * @param version to check against
     * @param persistent whether map is backed by persistent storage
     * @return true if this root has unsaved changes
     */
    boolean hasChangesSince(long version, boolean persistent) {
        return persistent && (root.isSaved() ? getAppendCounter() > 0 : getTotalCount() > 0)
                || getVersion() > version; // root 版本大于给定的 version
    }

    int getAppendCounter() {
        return appendCounter & 0xff;
    }

    /**
     * Whether flushing is needed.
     *
     * @return true if yes
     */
    public boolean needFlush() {
        return appendCounter != 0;
    }

    public long getTotalCount() {
        return root.getTotalCount() + getAppendCounter();
    }

    @Override
    public String toString() {
        return "RootReference(" + System.identityHashCode(root) +
                ", v=" + version +
                ", owner=" + ownerId + (ownerId == Thread.currentThread().getId() ? "(current)" : "") +
                ", holdCnt=" + holdCount +
                ", keys=" + root.getTotalCount() +
                ", append=" + getAppendCounter() +
                ")";
    }
}
