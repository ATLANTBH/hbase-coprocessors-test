package com.abh.hbase.coprocessors.batchops;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;


/**
 * Defines batch operation to execute.
 * 
 * Rows to be affected by batch operation
 * are defined by {@link Scan}.
 * 
 * Columns and/or values to be deleted/updated are 
 * defined in a list of {@link Value} objects
 *
 */
public class BatchOperation implements Writable {

  private Scan scan;
  private List<Value> values = new ArrayList<Value>();

  public BatchOperation() {
  }

  public BatchOperation(Scan scan) {
    this.scan = scan;
  }

  public void addValue(byte[] family, byte[] qualifier, byte[] value) {
    this.values.add(new Value(family, qualifier, value));
  }
  
  public void addValue(Value value){
    this.values.add(value);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HbaseObjectWritable.writeObject(out, scan, scan.getClass(), null);
    out.writeInt(values.size());
    for (Value value : values) {
      HbaseObjectWritable.writeObject(out, value, value.getClass(), null);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    scan = (Scan) HbaseObjectWritable.readObject(in, null);
    int numValues = in.readInt();
    for (int i = 0; i < numValues; i++) {
      values.add((Value) HbaseObjectWritable.readObject(in, null));
    }

  }

  public Scan getScan() {
    return scan;
  }

  public void setScan(Scan scan) {
    this.scan = scan;
  }
  
  
  public List<Value> getValues(){
    return this.values;
  }
  
  
  public static class Value implements Writable{
    
    private byte[] family;
    private byte[] qualifier;
    private byte[] value;
    
    private int familyLength;
    private int qualifierLenght;
    private int valueLenght;
    private long timestamp = Long.MAX_VALUE;
    
    public Value(){}
    
    public Value(byte[] family, byte[] qualifier, byte[] value){
      this.family = family;
      this.familyLength = family.length;
      this.qualifier = qualifier;
      this.qualifierLenght = qualifier.length;
      this.value = value;
      this.valueLenght = value.length;
    }
    
    public Value(byte[] family, byte[] qualifier, byte[] value, long timestamp){
      this(family, qualifier, value);
      this.timestamp = timestamp;
    }
    
    
    @Override
    public void readFields(DataInput in) throws IOException {
      this.familyLength = in.readInt();
      this.family = new byte[familyLength];
      in.readFully(family);
      
      this.qualifierLenght = in.readInt();
      this.qualifier = new byte[qualifierLenght];
      in.readFully(qualifier);
      
      this.valueLenght = in.readInt();
      this.value = new byte[valueLenght];
      in.readFully(value);
      
      this.timestamp = in.readLong();
      
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(this.familyLength);
      out.write(this.family);
      
      out.writeInt(this.qualifierLenght);
      out.write(this.qualifier);
      
      out.writeInt(this.valueLenght);
      out.write(this.value);
      
      out.writeLong(timestamp);
    }
    
    public byte[] getFamily() {
      return family;
    }

    public void setFamily(byte[] family) {
      this.family = family;
    }

    public byte[] getQualifier() {
      return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
      this.qualifier = qualifier;
    }

    public byte[] getValue() {
      return value;
    }

    public void setValue(byte[] value) {
      this.value = value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
    
  }

}
