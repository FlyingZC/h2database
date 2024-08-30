/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Class RandomAccessStore.
 * <UL>
 * <LI> 4/5/20 2:51 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public abstract class RandomAccessStore extends FileStore<SFChunk>
{
    /**
     * The free spaces between the chunks. The first block to use is block 2
     * (the first two blocks are the store header).
     */
    protected final FreeSpaceBitSet freeSpace = new FreeSpaceBitSet(2, BLOCK_SIZE);

    /**
     * Allocation mode:
     * false - new chunk is always allocated at the end of file
     * true - new chunk is allocated as close to the beginning of file, as possible
     */
    private volatile boolean reuseSpace = true;


    private long reservedLow;
    private long reservedHigh;
    private boolean stopIdleHousekeeping;

    public RandomAccessStore(Map<String, Object> config) {
        super(config);
    }

    @Override
    protected final SFChunk createChunk(int newChunkId) {
        return new SFChunk(newChunkId);
    }

    @Override
    public SFChunk createChunk(String s) {
        return new SFChunk(s);
    }

    @Override
    protected SFChunk createChunk(Map<String, String> map) {
        return new SFChunk(map);
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    @Override
    public void markUsed(long pos, int length) {
        freeSpace.markUsed(pos, length);
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the start position in bytes
     */
    private long allocate(int length, long reservedLow, long reservedHigh) {
        return freeSpace.allocate(length, reservedLow, reservedHigh);
    }

    /**
     * Calculate starting position of the prospective allocation.
     *
     * @param blocks the number of blocks to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the starting block index
     */
    private long predictAllocation(int blocks, long reservedLow, long reservedHigh) {
        return freeSpace.predictAllocation(blocks, reservedLow, reservedHigh);
    }

    @Override
    public boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory) {
        return unsavedMemory > autoCommitMemory;
    }

    private boolean isFragmented() {
        return freeSpace.isFragmented();
    }

    @Override
    public boolean isSpaceReused() {
        return reuseSpace;
    }

    @Override
    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    @Override
    protected void freeChunkSpace(Iterable<SFChunk> chunks) {
        for (SFChunk chunk : chunks) {
            freeChunkSpace(chunk);
        }
        assert validateFileLength(String.valueOf(chunks));
    }

    private void freeChunkSpace(SFChunk chunk) {
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        free(start, length);
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    protected void free(long pos, int length) {
        freeSpace.free(pos, length);
    }

    @Override
    public int getFillRate() {
        saveChunkLock.lock();
        try {
            return freeSpace.getFillRate();
        } finally {
            saveChunkLock.unlock();
        }
    }

    @Override
    protected final boolean validateFileLength(String msg) {
        assert saveChunkLock.isHeldByCurrentThread();
        assert getFileLengthInUse() == measureFileLengthInUse() :
                getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + msg;
        return true;
    }

    private long measureFileLengthInUse() {
        assert saveChunkLock.isHeldByCurrentThread();
        long size = 2;
        for (SFChunk c : getChunks().values()) {
            if (c.isAllocated()) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    long getFirstFree() {
        return freeSpace.getFirstFree();
    }

    long getFileLengthInUse() {
        return freeSpace.getLastFree();
    }

    @Override
    protected void readStoreHeader(boolean recoveryMode) {
        SFChunk newest = null;
        boolean assumeCleanShutdown = true; // 假设存储文件是在干净状态下关闭的
        boolean validStoreHeader = false; // 存储文件头部是否有效的标志
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = readFully((SFChunk)null, 0, 2 * FileStore.BLOCK_SIZE); // 读取前两个块的数据到 ByteBuffer 中
        byte[] buff = new byte[FileStore.BLOCK_SIZE]; // 创建一个字节数组，用于存放单个块的内容
        for (int i = 0; i <= FileStore.BLOCK_SIZE; i += FileStore.BLOCK_SIZE) { // 1.读取两个文件头，i+=BLOCK_SIZE
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                HashMap<String, String> m = DataUtils.parseChecksummedMap(buff); // 1.1.解析文件头属性map,里面会做checksum校验
                if (m == null) {
                    assumeCleanShutdown = false; // 如果 Map 解析失败，则认为不是干净关机
                    continue;
                }
                long version = DataUtils.readHexLong(m, FileStore.HDR_VERSION, 0); // 1.1.1读取属性map里的version
                // if both header blocks do agree on version
                // we'll continue on happy path - assume that previous shutdown was clean
                assumeCleanShutdown = assumeCleanShutdown && (newest == null || version == newest.version); // 如果两个头部块的版本号一致，则继续认为是干净关机
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m); // 存储 header
                    int chunkId = DataUtils.readHexInt(m, FileStore.HDR_CHUNK, 0); // 1.1.2.读取文件头里的 chunk id
                    long block = DataUtils.readHexLong(m, FileStore.HDR_BLOCK, 2); // 1.1.3.读取文件头里的 block 位置
                    SFChunk test = readChunkHeaderAndFooter(block, chunkId); // 1.2.根据上面文件头里的 chunk 和 block,定位并读取 chunk 的头部和尾部信息，并进行校验
                    if (test != null) {
                        newest = test; // 1.3.更新最新的 chunk
                    }
                }
            } catch (Exception ignore) {
                assumeCleanShutdown = false;
            }
        }

        if (!validStoreHeader) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", this);
        }

        processCommonHeaderAttributes(); // 2.处理通用的头部属性

        assumeCleanShutdown = assumeCleanShutdown && newest != null && !recoveryMode; // 再次确认是否为干净关机
        if (assumeCleanShutdown) {
            assumeCleanShutdown = DataUtils.readHexInt(storeHeader, FileStore.HDR_CLEAN, 0) != 0; // 读取是否为干净关机的标记
        }
//        assert getChunks().size() <= 1 : getChunks().size();

        long fileSize = size(); // 计算文件大小和块的数量
        long blocksInStore = fileSize / FileStore.BLOCK_SIZE;

        Comparator<SFChunk> chunkComparator = (one, two) -> { // 定义一个比较器，用于比较 SFChunk 对象
            int result = Long.compare(two.version, one.version); // 先按版本号降序比较
            if (result == 0) {
                // out of two copies of the same chunk we prefer the one
                // close to the beginning of file (presumably later version)
                result = Long.compare(one.block, two.block); // 如果版本号相同，则按块位置升序比较
            }
            return result;
        };

        Map<Long,SFChunk> validChunksByLocation = new HashMap<>(); // 保存有效的 chunk 信息
        if (assumeCleanShutdown) {
            // quickly check latest 20 chunks referenced in meta table
            Queue<SFChunk> chunksToVerify = new PriorityQueue<>(20, Collections.reverseOrder(chunkComparator)); // 快速检查最近的 20 个在元数据表中引用的 chunk
            try {
                setLastChunk(newest); // 设置最新的 chunk(1.磁盘读取chunk(root page);2.创建 root page;3.设置 root page 到 mvMap)
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                for (SFChunk c : getChunksFromLayoutMap()) {
                    // might be there already, due to meta traversal
                    // see readPage() ... getChunkIfFound()
                    chunksToVerify.offer(c); // 将 chunk 添加到队列中
                    if (chunksToVerify.size() == 20) {
                        chunksToVerify.poll(); // 如果队列已满，则移除最早的元素
                    }
                }
                SFChunk c;
                while (assumeCleanShutdown && (c = chunksToVerify.poll()) != null) {
                    SFChunk test = readChunkHeaderAndFooter(c.block, c.id); // 校验 chunk header & footer
                    assumeCleanShutdown = test != null;
                    if (assumeCleanShutdown) {
                        validChunksByLocation.put(test.block, test); // 有效的 chunk
                    }
                }
            } catch(IllegalStateException ignored) {
                assumeCleanShutdown = false;
            }
        } else {
            SFChunk tailChunk = discoverChunk(blocksInStore);
            if (tailChunk != null) {
                blocksInStore = tailChunk.block; // for a possible full scan later on
                validChunksByLocation.put(blocksInStore, tailChunk);
                if (newest == null || tailChunk.version > newest.version) {
                    newest = tailChunk;
                }
            }
            if (newest != null) {
                // read the chunk header and footer,
                // and follow the chain of next chunks
                while (true) {
                    validChunksByLocation.put(newest.block, newest);
                    if (newest.next == 0 || newest.next >= blocksInStore) {
                        // no (valid) next
                        break;
                    }
                    SFChunk test = readChunkHeaderAndFooter(newest.next, (newest.id + 1) & Chunk.MAX_ID);
                    if (test == null || test.version <= newest.version) {
                        break;
                    }
                    newest = test;
                }
            }
        }

        if (!assumeCleanShutdown) {
            // now we know, that previous shutdown did not go well and file
            // is possibly corrupted but there is still hope for a quick
            // recovery
            boolean quickRecovery = !recoveryMode &&
                    findLastChunkWithCompleteValidChunkSet(chunkComparator, validChunksByLocation, false);
            if (!quickRecovery) {
                // scan whole file and try to fetch chunk header and/or footer out of every block
                // matching pairs with nothing in-between are considered as valid chunk
                long block = blocksInStore;
                SFChunk tailChunk;
                while ((tailChunk = discoverChunk(block)) != null) {
                    block = tailChunk.block;
                    validChunksByLocation.put(block, tailChunk);
                }

                if (!findLastChunkWithCompleteValidChunkSet(chunkComparator, validChunksByLocation, true)
                        && hasPersistentData()) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "File is corrupted - unable to recover a valid set of chunks");
                }
            }
        }

        clear();
        // build the free space list
        for (SFChunk c : getChunks().values()) {
            if (c.isAllocated()) {
                long start = c.block * FileStore.BLOCK_SIZE;
                int length = c.len * FileStore.BLOCK_SIZE;
                markUsed(start, length);
            }
            if (!c.isLive()) {
                registerDeadChunk(c);
            }
        }
        assert validateFileLength("on open");
    }

    @Override
    protected void initializeStoreHeader(long time) {
        initializeCommonHeaderAttributes(time); // 1.初始化header属性
        writeStoreHeader(); // 2.写出header到存储，可以打开16进制文本查看 ~/h2_test/data/test/TestMVStore
    }

    @Override
    protected final void allocateChunkSpace(SFChunk chunk, WriteBuffer buff) {
        long reservedLow = this.reservedLow;
        long reservedHigh = this.reservedHigh > 0 ? this.reservedHigh : isSpaceReused() ? 0 : getAfterLastBlock();
        long filePos = allocate(buff.limit(), reservedLow, reservedHigh);
        // calculate and set the likely next position
        if (reservedLow > 0 || reservedHigh == reservedLow) {
            chunk.next = predictAllocation(chunk.len, 0, 0);
        } else {
            // just after this chunk
            chunk.next = 0;
        }
        chunk.block = filePos / BLOCK_SIZE;
    }

    @Override
    protected final void writeChunk(SFChunk chunk, WriteBuffer buffer) {
        long filePos = chunk.block * BLOCK_SIZE;
        writeFully(chunk, filePos, buffer.getBuffer());

        // end of the used space is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + buffer.limit() >= size();
        boolean shouldWriteStoreHeader = shouldWriteStoreHeader(chunk, storeAtEndOfFile);
        lastChunk = chunk;
        if (shouldWriteStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkStoreIfPossible(1);
        }
    }

    private boolean shouldWriteStoreHeader(SFChunk c, boolean storeAtEndOfFile) {
        // whether we need to write the store header
        boolean writeStoreHeader = false;
        if (!storeAtEndOfFile) {
            SFChunk chunk = lastChunk;
            if (chunk == null) {
                writeStoreHeader = true;
            } else if (chunk.next != c.block) {
                // the last prediction did not match
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(storeHeader, HDR_VERSION, 0);
                if (chunk.version - headerVersion > 20) {
                    // we write after at least every 20 versions
                    writeStoreHeader = true;
                } else {
                    for (int chunkId = DataUtils.readHexInt(storeHeader, HDR_CHUNK, 0);
                            !writeStoreHeader && chunkId <= chunk.id; ++chunkId) {
                        // one of the chunks in between
                        // was removed
                        writeStoreHeader = !getChunks().containsKey(chunkId);
                    }
                }
            }
        }

        if (storeHeader.remove(HDR_CLEAN) != null) {
            writeStoreHeader = true;
        }
        return writeStoreHeader;
    }

    @Override
    protected final void writeCleanShutdownMark() {
        shrinkStoreIfPossible(0);
        storeHeader.put(HDR_CLEAN, 1);
        writeStoreHeader();
    }

    @Override
    protected final void adjustStoreToLastChunk() {
        storeHeader.put(HDR_CLEAN, 1);
        writeStoreHeader();
        readStoreHeader(false);
    }

    /**
     * Compact store file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param thresholdFillRate do not compact if store fill rate above this value (0-100)
     * @param maxCompactTime the maximum time in milliseconds to compact
     * @param maxWriteSize the maximum amount of data to be written as part of this call
     */
    @Override
    protected void compactStore(int thresholdFillRate, long maxCompactTime, int maxWriteSize, MVStore mvStore) {
        setRetentionTime(0);
        long stopAt = System.nanoTime() + maxCompactTime * 1_000_000L;
        while (compact(thresholdFillRate, maxWriteSize)) {
            sync();
            compactMoveChunks(thresholdFillRate, maxWriteSize, mvStore);
            if (System.nanoTime() - stopAt > 0L) {
                break;
            }
        }
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     * @param mvStore owner of this store
     */
    public void compactMoveChunks(int targetFillRate, long moveSize, MVStore mvStore) {
        if (isSpaceReused()) {
            mvStore.executeFilestoreOperation(() -> {
                dropUnusedChunks();
                saveChunkLock.lock();
                try {
                    if (hasPersistentData() && getFillRate() <= targetFillRate) {
                        compactMoveChunks(moveSize);
                    }
                } finally {
                    saveChunkLock.unlock();
                }
            });
        }
    }

    private void compactMoveChunks(long moveSize) {
        long start = getFirstFree() / FileStore.BLOCK_SIZE;
        Iterable<SFChunk> chunksToMove = findChunksToMove(start, moveSize);
        if (chunksToMove != null) {
            compactMoveChunks(chunksToMove);
        }
    }

    private Iterable<SFChunk> findChunksToMove(long startBlock, long moveSize) {
        long maxBlocksToMove = moveSize / FileStore.BLOCK_SIZE;
        Iterable<SFChunk> result = null;
        if (maxBlocksToMove > 0) {
            PriorityQueue<SFChunk> queue = new PriorityQueue<>(getChunks().size() / 2 + 1,
                    (o1, o2) -> {
                        // instead of selection just closest to beginning of the file,
                        // pick smaller chunk(s) which sit in between bigger holes
                        int res = Integer.compare(o2.collectPriority, o1.collectPriority);
                        if (res != 0) {
                            return res;
                        }
                        return Long.signum(o2.block - o1.block);
                    });
            long size = 0;
            for (SFChunk chunk : getChunks().values()) {
                if (chunk.isAllocated() && chunk.block > startBlock) {
                    chunk.collectPriority = getMovePriority(chunk);
                    queue.offer(chunk);
                    size += chunk.len;
                    while (size > maxBlocksToMove) {
                        Chunk<?> removed = queue.poll();
                        if (removed == null) {
                            break;
                        }
                        size -= removed.len;
                    }
                }
            }
            if (!queue.isEmpty()) {
                ArrayList<SFChunk> list = new ArrayList<>(queue);
                list.sort(Chunk.PositionComparator.instance());
                result = list;
            }
        }
        return result;
    }

    private int getMovePriority(SFChunk chunk) {
        return getMovePriority((int)chunk.block);
    }

    private void compactMoveChunks(Iterable<SFChunk> move) {
        assert saveChunkLock.isHeldByCurrentThread();
        if (move != null) {
            // this will ensure better recognition of the last chunk
            // in case of power failure, since we are going to move older chunks
            // to the end of the file
            writeStoreHeader();
            sync();

            Iterator<SFChunk> iterator = move.iterator();
            assert iterator.hasNext();
            long leftmostBlock = iterator.next().block;
            long originalBlockCount = getAfterLastBlock();
            // we need to ensure that chunks moved within the following loop
            // do not overlap with space just released by chunks moved before them,
            // hence the need to reserve this area [leftmostBlock, originalBlockCount)
            for (SFChunk chunk : move) {
                moveChunk(chunk, leftmostBlock, originalBlockCount);
            }
            // update the metadata (hopefully within the file)
            store(leftmostBlock, originalBlockCount);
            sync();

            SFChunk chunkToMove = lastChunk;
            assert chunkToMove != null;
            long postEvacuationBlockCount = getAfterLastBlock();

            boolean chunkToMoveIsAlreadyInside = chunkToMove.block < leftmostBlock;
            boolean movedToEOF = !chunkToMoveIsAlreadyInside;
            // move all chunks, which previously did not fit before reserved area
            // now we can re-use previously reserved area [leftmostBlock, originalBlockCount),
            // but need to reserve [originalBlockCount, postEvacuationBlockCount)
            for (SFChunk c : move) {
                if (c.block >= originalBlockCount &&
                        moveChunk(c, originalBlockCount, postEvacuationBlockCount)) {
                    assert c.block < originalBlockCount;
                    movedToEOF = true;
                }
            }
            assert postEvacuationBlockCount >= getAfterLastBlock();

            if (movedToEOF) {
                boolean moved = moveChunkInside(chunkToMove, originalBlockCount);

                // store a new chunk with updated metadata (hopefully within a file)
                store(originalBlockCount, postEvacuationBlockCount);
                sync();
                // if chunkToMove did not fit within originalBlockCount (move is
                // false), and since now previously reserved area
                // [originalBlockCount, postEvacuationBlockCount) also can be
                // used, lets try to move that chunk into this area, closer to
                // the beginning of the file
                long lastBoundary = moved || chunkToMoveIsAlreadyInside ?
                                        postEvacuationBlockCount : chunkToMove.block;
                moved = !moved && moveChunkInside(chunkToMove, lastBoundary);
                if (moveChunkInside(lastChunk, lastBoundary) || moved) {
                    store(lastBoundary, -1);
                }
            }

            shrinkStoreIfPossible(0);
            sync();
        }
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder(112);
        if (hasPersistentData()) {
            storeHeader.put(HDR_BLOCK, lastChunk.block);
            storeHeader.put(HDR_CHUNK, lastChunk.id);
            storeHeader.put(HDR_VERSION, lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, HDR_FLETCHER, checksum);
        buff.append('\n');
        bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        writeFully(null, 0, header);
    }

    private void store(long reservedLow, long reservedHigh) {
        this.reservedLow = reservedLow;
        this.reservedHigh = reservedHigh;
        saveChunkLock.unlock();
        try {
            store();
        } finally {
            saveChunkLock.lock();
            this.reservedLow = 0;
            this.reservedHigh = 0;
        }
    }

    private boolean moveChunkInside(SFChunk chunkToMove, long boundary) {
        boolean res = chunkToMove.block >= boundary &&
                predictAllocation(chunkToMove.len, boundary, -1) < boundary &&
                moveChunk(chunkToMove, boundary, -1);
        assert !res || chunkToMove.block + chunkToMove.len <= boundary;
        return res;
    }

    /**
     * Move specified chunk into free area of the file. "Reserved" area
     * specifies file interval to be avoided, when un-allocated space will be
     * chosen for a new chunk's location.
     *
     * @param chunk            to move
     * @param reservedAreaLow  low boundary of reserved area, inclusive
     * @param reservedAreaHigh high boundary of reserved area, exclusive
     * @return true if block was moved, false otherwise
     */
    private boolean moveChunk(SFChunk chunk, long reservedAreaLow, long reservedAreaHigh) {
        // ignore if already removed during the previous store operations
        // those are possible either as explicit commit calls
        // or from meta map updates at the end of this method
        if (!getChunks().containsKey(chunk.id)) {
            return false;
        }
        long start = chunk.block * FileStore.BLOCK_SIZE;
        int length = chunk.len * FileStore.BLOCK_SIZE;
        long pos = allocate(length, reservedAreaLow, reservedAreaHigh);
        long block = pos / FileStore.BLOCK_SIZE;
        // in the absence of a reserved area,
        // block should always move closer to the beginning of the file
        assert reservedAreaHigh > 0 || block <= chunk.block : block + " " + chunk;
        ByteBuffer readBuff = readFully(chunk, start, length);
        writeFully(null, pos, readBuff);
        free(start, length);
        // can not set chunk's new block/len until it's fully written at new location,
        // because concurrent reader can pick it up prematurely,
        chunk.block = block;
        chunk.next = 0;
        saveChunkMetadataChanges(chunk);
        return true;
    }

    /**
     * Shrink the store if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    @Override
    protected void shrinkStoreIfPossible(int minPercent) {
        assert saveChunkLock.isHeldByCurrentThread();
        long result = getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        shrinkIfPossible(minPercent);
    }

    private void shrinkIfPossible(int minPercent) {
        if (isReadOnly()) {
            return;
        }
        long end = getFileLengthInUse();
        long fileSize = size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        sync();
        truncate(end);
    }

    @Override
    protected void doHousekeeping(MVStore mvStore) throws InterruptedException {
        boolean idle = isIdle();
        int rewritableChunksFillRate = getRewritableChunksFillRate();
        if (idle && stopIdleHousekeeping) {
            return;
        }
        int autoCommitMemory = mvStore.getAutoCommitMemory();
        int fileFillRate = getFillRate();
        long chunksTotalSize = size() * fileFillRate / 100;
        if (isFragmented() && fileFillRate < getAutoCompactFillRate()) {
            mvStore.tryExecuteUnderStoreLock(() -> {
                int moveSize = 2 * autoCommitMemory;
                if (idle) {
                    moveSize *= 4;
                }
                compactMoveChunks(101, moveSize, mvStore);
                return true;
            });
        }

        int chunksFillRate = getChunksFillRate();
        int adjustedUpFillRate = 50 + rewritableChunksFillRate / 2;
        int fillRateToCompare = idle ? rewritableChunksFillRate : adjustedUpFillRate;
        if (fillRateToCompare < getTargetFillRate(idle)) {
            int targetFillRate = idle ? adjustedUpFillRate : rewritableChunksFillRate;
            mvStore.tryExecuteUnderStoreLock(() -> {
                int writeLimit = autoCommitMemory;
                if (!idle) {
                    writeLimit /= 4;
                }
                if (rewriteChunks(writeLimit, targetFillRate)) {
                    dropUnusedChunks();
                }
                return true;
            });
        }
        stopIdleHousekeeping = false;
        if (idle) {
            int currentChunksFillRate = getChunksFillRate();
            long currentTotalChunksSize = size() * getFillRate() / 100;
            stopIdleHousekeeping = currentTotalChunksSize > chunksTotalSize
                    || currentTotalChunksSize == chunksTotalSize && currentChunksFillRate <= chunksFillRate;
        }
    }

    private int getTargetFillRate(boolean idle) {
        int targetRate = getAutoCompactFillRate();
        // use a lower fill rate if there were any file operations since the last time
        if (!idle) {
            targetRate = targetRate * targetRate / 100;
        }
        return targetRate;
    }

    protected abstract void truncate(long size);

    /**
     * Mark the file as empty.
     */
    @Override
    public void clear() {
        freeSpace.clear();
    }

    /**
     * Calculates relative "priority" for chunk to be moved.
     *
     * @param block where chunk starts
     * @return priority, bigger number indicate that chunk need to be moved sooner
     */
    public int getMovePriority(int block) {
        return freeSpace.getMovePriority(block);
    }

    /**
     * Get the index of the first block after last occupied one.
     * It marks the beginning of the last (infinite) free space.
     *
     * @return block index
     */
    private long getAfterLastBlock() {
        assert saveChunkLock.isHeldByCurrentThread();
        return getAfterLastBlock_();
    }

    protected long getAfterLastBlock_() {
        return freeSpace.getAfterLastBlock();
    }

    @Override
    public Collection<SFChunk> getRewriteCandidates() {
        return isSpaceReused() ? null : Collections.emptyList();
    }
}
