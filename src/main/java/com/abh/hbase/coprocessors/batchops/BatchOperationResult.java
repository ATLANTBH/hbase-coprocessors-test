package com.abh.hbase.coprocessors.batchops;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Defines result of batch operation.
 * 
 * Each batch operation returns number of
 * records affected, and time that execution took.
 *
 */
public class BatchOperationResult implements Writable {

  public BatchOperationResult() {
  }

  public BatchOperationResult(long time, long records) {
    this.time = time;
    this.records = records;
  }

  private long time;
  private long records;

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getRecords() {
    return records;
  }

  public void setRecords(long records) {
    this.records = records;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    time = in.readLong();
    records = in.readLong();

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(time);
    out.writeLong(records);

  }

}
