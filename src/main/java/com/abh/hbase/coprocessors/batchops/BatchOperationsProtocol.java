package com.abh.hbase.coprocessors.batchops;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * 
 * @author davor
 * 
 */
public interface BatchOperationsProtocol extends CoprocessorProtocol{

	/**
	 * 
	 * @param scan
	 * @param put
	 * @return
	 */
	BatchOperationResult batchUpdate(BatchOperation update) throws IOException;
	
	/**
	 * 
	 * @param delete
	 * @return
	 * @throws IOException
	 */
	BatchOperationResult batchDelete(BatchOperation delete) throws IOException;

}
