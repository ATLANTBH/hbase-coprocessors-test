package com.abh.hbase.coprocessors.tablereplica;

import static com.abh.hbase.coprocessors.tablereplica.TableReplicationConstants.*;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

public class TableReplicationMasterObserver extends BaseMasterObserver {

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    super.start(ctx);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    // add TableReplicationObserver
    // to the list of coprocessors
    // for the newly created table
    if (!isAlreadyReplica(desc, ctx.getEnvironment())) {
      desc.addCoprocessor(TableReplicationRegionObserver.class.getName());
    }
  }

  @Override
  public void postCreateTable(
      ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
      HRegionInfo[] regions) throws IOException {

    if (isAlreadyReplica(desc, ctx.getEnvironment())) {
      return;
    }
    HTableDescriptor replicaTable = new HTableDescriptor();
    replicaTable.setName(Bytes.toBytes(desc.getNameAsString().concat(
        ctx.getEnvironment().getConfiguration()
            .get(REPLICA_TABLE_POSTFIX, "_replica"))));
    ctx.getEnvironment().getMasterServices().createTable(replicaTable, null);
    for (HColumnDescriptor family : desc.getFamilies()) {
      replicaTable.addFamily(family);
    }
  }

  private boolean isAlreadyReplica(HTableDescriptor desc,
      CoprocessorEnvironment env) throws IOException {
    if (desc.getNameAsString().contains(
        env.getConfiguration().get(REPLICA_TABLE_POSTFIX, "_replica"))) {
      return true;
    } else {
      return false;
    }
  }
}
