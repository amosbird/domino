/**
 *  Domino, A Transaction Engine Based on Apache HBase
 *  Copyright (C) 2014  Zhen Zhao
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package ict.wde.domino.cop;

import ict.wde.domino.common.DominoConst;
import ict.wde.domino.common.TMetaIface;
import ict.wde.domino.common.Version;
import ict.wde.domino.id.DominoIdIface;
import ict.wde.domino.id.DominoIdService;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction Metadata Endpoint Implementation.
 *
 * @author Zhen Zhao, ICT, CAS
 *
 */
public class TMetaEndpoint implements TMetaIface {

  static final Logger LOG = LoggerFactory.getLogger(TMetaEndpoint.class);

  private HRegion region;
  private HTableInterface metaTable = null;
  private DominoIdIface tidClient;

  private Configuration conf = null;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("-------------TrxMetaEndpoint starting, version:{} ------------",
        Version.VERSION);
    conf = env.getConfiguration();
    tidClient = DominoIdService.getClient(conf.get(DominoConst.ZK_PROP));
    this.region = ((RegionCoprocessorEnvironment) env).getRegion();
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    if (metaTable != null) metaTable.close();
    // this.env = null;
    // this.conf = null;
    this.region = null;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2)
      throws IOException {
    return new ProtocolSignature(Version.VERSION, null);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return Version.VERSION;
  }

  public long getCommitId(byte[] startId) throws IOException {
    long commitId = tidClient.getId();
    Get get = new Get(startId);
    Result r = region.get(get);
    if (DominoConst.TRX_ACTIVE != DominoConst.transactionStatus(r)) {
      return DominoConst.ERR_TRX_ABORTED;
    }
    return commitId;
  }

  public void commitTransaction(byte[] startId, long commitId) throws IOException {
    long startIdLong = DominoConst.getTidFromTMetaKey(startId);
    Put put = new Put(startId);
    put.add(DominoConst.TRANSACTION_META_FAMILY,
        DominoConst.TRANSACTION_STATUS, startIdLong,
        DominoConst.TRX_COMMITTED_B);
    put.add(DominoConst.TRANSACTION_META_FAMILY,
        DominoConst.TRANSACTION_COMMIT_ID, startIdLong,
        Bytes.toBytes(commitId));
    region.put(put, false);
  }

  @Override
  public void abortTransaction(byte[] startId) throws IOException {
    abort(startId);
  }

  @SuppressWarnings("deprecation")
  private void abort(byte[] startId) throws IOException {
    Put put = new Put(startId);
    long startIdLong = DominoConst.getTidFromTMetaKey(startId);
    put.add(DominoConst.TRANSACTION_META_FAMILY,
        DominoConst.TRANSACTION_STATUS, startIdLong, DominoConst.TRX_ABORTED_B);
    region.put(put, false);
  }

  @SuppressWarnings("deprecation")
  @Override
  public Result getTransactionStatus(long transactionId) throws IOException {
    byte[] row = DominoConst.long2TranscationRowKey(transactionId);
    Get get = new Get(row);
    Result r = region.get(get);
    if (System.currentTimeMillis() - DominoConst.getLastTouched(r) > DominoConst.TRX_EXPIRED) {
      // If it's too long since the client last updated the transaction
      // timestamp, the client may be no longer alive.
      // So we have to mark the transaction as aborted and let the caller
      // clear the row status.
      abort(row);
      return region.get(get);
    }
    else {
      return r;
    }
  }

}
