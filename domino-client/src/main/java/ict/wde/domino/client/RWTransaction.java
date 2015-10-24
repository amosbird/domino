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

package ict.wde.domino.client;

import ict.wde.domino.common.DominoConst;
import ict.wde.domino.common.DominoIface;
import ict.wde.domino.common.TMetaIface;
import ict.wde.domino.common.writable.DResult;
import ict.wde.domino.id.DominoIdIface;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import jline.internal.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Common read&write transaction implementation.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class RWTransaction implements Transaction {

  /**
   * Struct-like class that stores the write history of the transaction.
   * 
   * @author Zhen Zhao, ICT, CAS
   * 
   */
  private static class Commit implements Iterable<Map.Entry<byte[], Boolean>> {
    final NavigableMap<byte[], Boolean> puts = new TreeMap<>(
        Bytes.BYTES_COMPARATOR);
    final HTableInterface table;

    Commit(final HTableInterface table) {
      this.table = table;
    }

    void add(byte[] row, boolean isDelete) {
      puts.put(row, isDelete);
    }

    @Override
    public Iterator<Entry<byte[], Boolean>> iterator() {
      return puts.entrySet().iterator();
    }

  }

  private final Configuration conf;
  private final long startId;
  private final DominoIdIface tidClient;
  private long commitId;
  private final Map<byte[], Commit> commits = new TreeMap<>(
      Bytes.BYTES_COMPARATOR);
  private final Map<byte[], HTableInterface> tables = new TreeMap<>(
      Bytes.BYTES_COMPARATOR);

  private boolean readyToCommit = true;

  protected RWTransaction(Configuration conf, DominoIdIface tidClient)
      throws IOException {
    this.conf = conf;
    this.tidClient = tidClient;
    this.startId = tidClient.getId();
  }

  private HTableInterface getTable(byte[] name) throws IOException {
    HTableInterface table = tables.get(name);
    if (table == null) {
      table = new HTable(conf, name);
      Field f;
      try {
        f = table.getClass().getDeclaredField("cleanupConnectionOnClose");
        f.setAccessible(true);
        f.set(table, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
      tables.put(name, table);
    }
    return table;
  }

  private void closeAllTables() {
    for (Map.Entry<byte[], HTableInterface> ent : tables.entrySet()) {
      try {
        ent.getValue().close();
      }
      catch (IOException ioe) {
      }
    }
    tables.clear();
  }

  public Result get(Get get, byte[] table) throws IOException {
    return get(get, getTable(table));
  }

  public Result get(Get get, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          get.getRow());
      DResult res = iface.get(get, startId);
      if (res.getErrMsg() != null) {
        throw new IOException(res.getErrMsg());
      }
      return res.getResult();
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public ResultScanner scan(Scan scan, byte[] table) throws IOException {
    return scan(scan, getTable(table));
  }

  public ResultScanner scan(Scan scan, HTableInterface table)
      throws IOException {
    checkIfReadyToContinue();
    if (scan.hasFamilies()) {
      scan.addFamily(DominoConst.INNER_FAMILY);
    }
    scan.setTimeRange(0, startId + 1);
    scan.setMaxVersions();
    return new DResultScanner(table.getScanner(scan), startId,
        table, this);
  }

  public void putStateful(Put put, byte[] table) throws IOException {
    putStateful(put, getTable(table));
  }

  public void putStateful(Put put, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          put.getRow());
      DResult res = iface.put(put, startId, true);
      if (res != null) {
        throw new IOException(res.getErrMsg());
      }
      addCommit(put.getRow(), false, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void put(Put put, byte[] table) throws IOException {
    put(put, getTable(table));
  }

  public void put(Put put, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          put.getRow());
      DResult res = iface.put(put, startId, false);
      if (res != null) {
        throw new IOException(res.getErrMsg());
      }
      addCommit(put.getRow(), false, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void delete(byte[] row, byte[] table) throws IOException {
    delete(row, getTable(table));
  }

  public void delete(byte[] row, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class, row);
      iface.delete(row, startId);
      addCommit(row, true, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void commit() throws IOException {
    checkIfReadyToContinue();
    getCommitId();
    commitPuts();
    closeAllTables();
  }

  public void rollback() throws IOException {
    readyToCommit = false;
    rollbackPuts();
    closeAllTables();
  }

  private void rollbackPuts() {
    for (byte[] key : commits.keySet()) {
      Commit commit = commits.get(key);
      for (Entry<byte[], Boolean> entry : commit) {
        byte[] row = entry.getKey();
        try {
          commit.table.coprocessorProxy(DominoIface.class, row).rollbackRow(
              row, startId);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void disable() {
    readyToCommit = false;
  }

  public void checkIfReadyToContinue() throws IOException {
    if (!readyToCommit) {
      throw new IOException(
          "This Transaction has to be aborted because of some earlier failure.");
    }
  }

  private void addCommit(byte[] row, boolean isDelete, HTableInterface table)
      throws IOException {
    Commit commit = commits.get(table.getTableName());
    if (commit == null) {
      commit = new Commit(getTable(table.getTableName()));
      commits.put(table.getTableName(), commit);
    }
    commit.add(row, isDelete);
  }

  private void commitPuts() {
    for (Commit commit : commits.values()) {
      for (Entry<byte[], Boolean> entry : commit) {
        byte[] row = entry.getKey();
        try {
          commit.table.coprocessorProxy(DominoIface.class, row).commitRow(row,
              startId, commitId, entry.getValue());
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void getCommitId() throws IOException {
    try {
      commitId = tidClient.getId();
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
  }

  @Override
  public long getStartId() {
    return startId;
  }

}
