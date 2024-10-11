/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/** 光标中的位置。  实例表示链表中的一个节点，它跟踪从叶节点内的特定（目标）键一直到根（自下而上的路径）的路径。
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * from a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public final class CursorPos<K,V> {

    /** 对应的 page
     * The page at the current level.
     */
    public Page<K,V> page;

    /** 对应 page 上的下标(负数表示插入下标)
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    public int index;

    /** 父节点位置.根节点的 parent pos 为 null
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    public CursorPos<K,V> parent;


    public CursorPos(Page<K,V> page, int index, CursorPos<K,V> parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param page      root of the tree
     * @param key       the key to search for
     * @return head of the CursorPos chain (insertion point)
     */
    static <K,V> CursorPos<K,V> traverseDown(Page<K,V> page, K key) {
        CursorPos<K,V> cursorPos = null;
        while (!page.isLeaf()) { // 当前页面不是叶子节点时，继续向下遍历
            int index = page.binarySearch(key) + 1; // 二分搜索
            if (index < 0) {
                index = -index;
            }
            cursorPos = new CursorPos<>(page, index, cursorPos);
            page = page.getChildPage(index); // 继续向下遍历到指定索引的子页面.如果子节点不在内存中,会从磁盘上加载到内存.
        }
        return new CursorPos<>(page, page.binarySearch(key), cursorPos); // 在叶子节点存储的 keys 中, 二分搜索 key 的位置
    }

    /**
     * Calculate the memory used by changes that are not yet stored.
     *
     * @param version the version
     * @return the amount of memory
     */
    int processRemovalInfo(long version) {
        int unsavedMemory = 0;
        for (CursorPos<K,V> head = this; head != null; head = head.parent) {
            unsavedMemory += head.page.removePage(version);
        }
        return unsavedMemory;
    }

    @Override
    public String toString() {
        return "CursorPos{" +
                "page=" + page +
                ", index=" + index +
                ", parent=" + parent +
                '}';
    }
}

