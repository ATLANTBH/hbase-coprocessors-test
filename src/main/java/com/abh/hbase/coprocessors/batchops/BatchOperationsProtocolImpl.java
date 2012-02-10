package com.abh.hbase.coprocessors.batchops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.abh.hbase.coprocessors.batchops.BatchOperation.Value;

public class BatchOperationsProtocolImpl extends BaseEndpointCoprocessor
    implements BatchOperationsProtocol {

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long version,
      int clientMethodsHashCode) throws IOException {
    if (BatchOperationsProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(1l, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  public BatchOperationResult batchUpdate(BatchOperation batchOperation)
      throws IOException {

    long records = 0;
    long t0 = System.currentTimeMillis();

    // set firstKeyOnlyFilter to get performance
    // improvement in the case no filter is set
    Scan scan = batchOperation.getScan();
    if (scan.getFilter() == null) {
      scan.setFilter(new FirstKeyOnlyFilter());
    }

    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);

    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean hasMoreRows = false;
    try {
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        if (!results.isEmpty()) {
          Put put = createPut(results.get(0).getRow(),
              batchOperation.getValues());
          ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
              .put(put);
          records++;
        }
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    return new BatchOperationResult(System.currentTimeMillis() - t0, records);
  }

  @Override
  public BatchOperationResult batchDelete(BatchOperation deleteOperation)
      throws IOException {

    long records = 0;
    long t0 = System.currentTimeMillis();

    Scan scan = deleteOperation.getScan();
    if (scan.getFilter() == null) {
      scan.setFilter(new FirstKeyOnlyFilter());
    }

    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean hasMoreRows = false;
    try {
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        if (!results.isEmpty()) {
          Delete delete = createDelete(results.get(0).getRow(),
              deleteOperation.getValues());
          ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().delete(
              delete, null, true);
          records++;
        }
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    return new BatchOperationResult(System.currentTimeMillis() - t0, records);
  }

  private Put createPut(byte[] row, List<Value> values) {
    Put put = new Put(row);
    for (Value value : values) {
      if (value.getTimestamp() < Long.MAX_VALUE) {
        put.add(value.getFamily(), value.getQualifier(), value.getTimestamp(),
            value.getValue());
      } else {
        put.add(value.getFamily(), value.getQualifier(), value.getValue());
      }
    }
    return put;
  }

  private Delete createDelete(byte[] row, List<Value> values) {
    Delete delete = new Delete(row);
    for (Value value : values) {
      if (value.getTimestamp() < Long.MAX_VALUE) {
        delete.deleteColumn(value.getFamily(), value.getQualifier(),
            value.getTimestamp());
      } else {
        delete.deleteColumn(value.getFamily(), value.getQualifier());
      }
    }
    return delete;
  }

}
