package com.abh.hbase.coprocessors.tablereplica;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class TableReplicationRegionObserver extends BaseRegionObserver {

	private HTablePool hTablePool;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		this.hTablePool = new HTablePool(e.getConfiguration(), 100,
				new EnvironmentBackedHTableInterfaceFactory(e));
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		this.hTablePool.close();
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> ctx,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		HTableInterface hTable = this.hTablePool.getTable(ctx.getEnvironment()
		    .getRegion().getTableDesc().getNameAsString()
				.concat(ctx.getEnvironment().getConfiguration()
				.get(TableReplicationConstants.REPLICA_TABLE_POSTFIX,
				"_replica")));
		try{
			hTable.put(put);
		}catch (IOException e) {
			throw e;
		}finally{
			//this returns table to the pool
			hTable.close();
		}
		

	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> ctx,
			Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		HTableInterface hTable = this.hTablePool.getTable(ctx.getEnvironment()
		    .getRegion().getTableDesc().getNameAsString()
				.concat(ctx.getEnvironment().getConfiguration()
				.get(TableReplicationConstants.REPLICA_TABLE_POSTFIX,
				"_replica")));
		try{
			hTable.delete(delete);
		}catch (IOException e) {
			throw e;
		}finally{
			hTable.close();
		}
	}

	public HTablePool gethTablePool() {
		return hTablePool;
	}

	public void sethTablePool(HTablePool hTablePool) {
		this.hTablePool = hTablePool;
	}

	public static class EnvironmentBackedHTableInterfaceFactory implements
			HTableInterfaceFactory {

		private CoprocessorEnvironment environment;

		public EnvironmentBackedHTableInterfaceFactory(
				CoprocessorEnvironment environment) {
			this.environment = environment;
		}

		@Override
		public HTableInterface createHTableInterface(Configuration config,
				byte[] tableName) {
			HTableInterface hTable = null;
			try {
				hTable = environment.getTable(tableName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return hTable;
		}

		@Override
		public void releaseHTableInterface(HTableInterface table)
				throws IOException {
			table.close();
		}

	}

}
