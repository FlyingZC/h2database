/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * Class LongDataType.
 * <UL>
 * <LI> 8/21/17 6:52 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class LongDataType extends BasicDataType<Long> {

    public static final LongDataType INSTANCE = new LongDataType();

    private static final Long[] EMPTY_LONG_ARR = new Long[0];

    private LongDataType() {}

    @Override
    public int getMemory(Long obj) {
        return 8;
    }

    @Override
    public void write(WriteBuffer buff, Long data) {
        buff.putVarLong(data);
    }

    @Override
    public Long read(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    @Override
    public Long[] createStorage(int size) {
        return size == 0 ? EMPTY_LONG_ARR : new Long[size];
    }

    @Override
    public int compare(Long one, Long two) {
        return Long.compare(one, two);
    }

    @Override
    public int binarySearch(Long keyObj, Object storageObj, int size, int initialGuess) {
        long key = keyObj;
        Long[] storage = cast(storageObj);
        int low = 0; // 下界
        int high = size - 1; // 上界
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1; // 猜测下标
        if (x < 0 || x > high) { // x小于0 或 大于high
            x = high >>> 1; // x 设置为 high 的一半 (无符号右移)
        }
        return binarySearch(key, storage, low, high, x); // 进行二分查找
    }

    private static int binarySearch(long key, Long[] storage, int low, int high, int x) { // 在已排序的Long数组storage中, 查找第一个值等于key的元素的位置
        while (low <= high) { // 使用循环不断将查找区间减半，直到low大于high
            long midVal = storage[x];
            if (key > midVal) {
                low = x + 1;
            } else if (key < midVal) {
                high = x - 1;
            } else {
                return x; // 找到则返回目标索引
            }
            x = (low + high) >>> 1; // 更新x为新的中间下标
        }
        return -(low + 1); // 找不到返回负数，表示插入位置(low 最终会比 high 大)
    }
}
