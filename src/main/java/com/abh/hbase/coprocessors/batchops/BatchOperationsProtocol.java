package com.abh.hbase.coprocessors.batchops;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Defines basic batch operations
 * 
 * Each method takes {@link BatchOperation} object as an argument and returns
 * {@link BatchOperationResult}
 * 
 */
public interface BatchOperationsProtocol extends CoprocessorProtocol {

  /**
   * Updates all rows defined by {@link Scan} object in BatchOperation argument
   * with values defined in list of {@link BatchOperation.Value} objects
   * 
   * @param update
   * @return
   * @throws IOException
   */
  BatchOperationResult batchUpdate(BatchOperation update) throws IOException;

  /**
   * Deletes from all rows defined by {@link Scan} object in BatchOperation
   * argument. Columns to delete are defined in list of
   * {@link BatchOperation.Value} objects
   * 
   * @param delete
   * @return
   * @throws IOException
   */
  BatchOperationResult batchDelete(BatchOperation delete) throws IOException;

}
