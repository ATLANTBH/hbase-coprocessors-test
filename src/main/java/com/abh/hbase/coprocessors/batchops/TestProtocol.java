package com.abh.hbase.coprocessors.batchops;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class TestProtocol {

  public static void main(String[] args) throws Throwable {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.session.timeout", "100000");
    
    HTableInterface hTable = new HTable(conf, "test");
    
    System.out.println("Putting");
    long t0 = System.currentTimeMillis();
//    for (int i = 0; i < 100000; i++) {
//      Put put = new Put(Bytes.toBytes("test" + i));
//      put.add(Bytes.toBytes("family1"), Bytes.toBytes("qual" + i), Bytes.toBytes("value" + i));
//      if(i % 10 ==0){
//        put.add(Bytes.toBytes("family1"), Bytes.toBytes("filterqual"), Bytes.toBytes("filter"));
//      }
//      hTable.put(put);
//    }
//    System.out.println("Issued puts in " + (System.currentTimeMillis() - t0));
    
    System.out.println("Executing Protocol call");
    t0 = System.currentTimeMillis();
    
    Scan scan = new Scan();
  //  SingleColumnValueFilter filter= new SingleColumnValueFilter(Bytes.toBytes("family1"), Bytes.toBytes("filterqual"),CompareOp.EQUAL, Bytes.toBytes("filter")); 
//    filter.setFilterIfMissing(true);
//    scan.setFilter(filter);
    final BatchOperation operation = new BatchOperation(scan);
    operation.addValue(Bytes.toBytes("family1"), Bytes.toBytes("first"), Bytes.toBytes("first val"));
    operation.addValue(Bytes.toBytes("family1"), Bytes.toBytes("second"), Bytes.toBytes("second val"));
    Map<byte[], BatchOperationResult> results = hTable.coprocessorExec(
        BatchOperationsProtocol.class, null, null,
        new Batch.Call<BatchOperationsProtocol, BatchOperationResult>() {

          @Override
          public BatchOperationResult call(BatchOperationsProtocol instance)
              throws IOException {
            return instance.batchUpdate(operation);
          }
        });
    System.out.println("Executed protocol in " + (System.currentTimeMillis() - t0));
    
    for (Map.Entry<byte[], BatchOperationResult> entry : results.entrySet()) {
      System.out.println(entry.getValue().getRecords() + " " + entry.getValue().getTime());
    }
  }
}
