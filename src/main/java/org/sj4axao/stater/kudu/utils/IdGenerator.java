package org.sj4axao.stater.kudu.utils;


/**
 * @author: Levon
 * @version: v 0.1 2018-03-12 15:33
 */
public class IdGenerator {

    //Mon Mar 12 15:41:17 CST 2018
    private final static long twepoch = 1520840477347L;             //日期起始点

    private final long workerId ;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    private final static long workerIdBits = 10L;                        //机器ID占用10bits
    private final static long sequenceBits = 12L;                        //序列占用12bits

    public final static long maxWorkerId = -1L ^ -1L << workerIdBits;  //机器ID 最大值

    private final static long timestampLeftShift = sequenceBits + workerIdBits;//时间偏移位
    private final static long workerIdShift = sequenceBits;               //机器ID偏移位

    public final static long sequenceMask = -1L ^ -1L << sequenceBits;  //序列掩码


    public IdGenerator(final long workerId) {
        super();
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        this.workerId = workerId;
    }

    public synchronized long nextId() {
        long timestamp = this.timeGen();
        if (this.lastTimestamp == timestamp) {
            this.sequence = (this.sequence + 1) & sequenceMask;
            if (this.sequence == 0) {
                timestamp = this.tilNextMillis(this.lastTimestamp);
            }
        } else {
            this.sequence = 0;
        }
        if (timestamp < this.lastTimestamp) {
            try {
                throw new Exception(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", this.lastTimestamp - timestamp));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.lastTimestamp = timestamp;
        long nextId = ((timestamp - twepoch << timestampLeftShift)) | (this.workerId << workerIdShift) | (this.sequence);
        return nextId;
    }

    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

}
