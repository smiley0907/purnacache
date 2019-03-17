/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPipelines {
  public static final Log LOG = LogFactory.getLog(TestPipelines.class);

  private static final short REPL_FACTOR = 3;
  private static final int RAND_LIMIT = 2000;
  private static final int FILE_SIZE = 10000;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static Configuration conf;
  static final Random rand = new Random(RAND_LIMIT);

  static {
    initLoggers();
    setConfiguration();
  }

  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
    fs = cluster.getFileSystem();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (fs != null)
      fs.close();
    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  /**
   * Creates and closes a file of certain length.
   * Calls append to allow next write() operation to add to the end of it
   * After write() invocation, calls hflush() to make sure that data sunk through
   * the pipeline and check the state of the last block's replica.
   * It supposes to be in RBW state
   *
   * @throws IOException in case of an error
   */
  @Test
  public void pipeline_01() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + METHOD_NAME);
    }
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    DFSTestUtil.createFile(fs, filePath, FILE_SIZE, REPL_FACTOR, rand.nextLong());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Invoking append but doing nothing otherwise...");
    }
    FSDataOutputStream ofs = fs.append(filePath);
    ofs.writeBytes("Some more stuff to write");
    ((DFSOutputStream) ofs.getWrappedStream()).hflush();

    List<LocatedBlock> lb = cluster.getNameNodeRpc().getBlockLocations(
      filePath.toString(), FILE_SIZE - 1, FILE_SIZE).getLocatedBlocks();
	  
	  DefaultDirectedGraph<String, DefaultEdge> directedGraph = new DefaultDirectedGraph<String, DefaultEdge>(
				DefaultEdge.class);
		
		for (int i = 0; i < n; i++)
			directedGraph.addVertex(String.valueOf(i)); // end of vertices addition
		
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (i == j) {
					continue;
				}
				directedGraph.addEdge(String.valueOf(i), String.valueOf(j)); // end of edges addition to form a graph
			}
		} // end of graph creation
		
		List<Node> nodes = new ArrayList<Node>();
		nodes.add(new Node("A", "A Data"));
		nodes.add(new Node("B", "Empty Space"));
		nodes.add(new Node("C", "Empty Space"));
		nodes.add(new Node("D", "Empty Space"));
		nodes.add(new Node("E", "Empty Space"));
		nodes.add(new Node("F", "Empty Space"));
		nodes.add(new Node("G", "Empty Space"));

		//System.out.println("Time before program start : " + Util.getDate());

		System.out.println("Data before copying");
		for (Node node : nodes) {
			System.out.println(node.toString());
		}

		Runnable runnable = new MyThread(nodes, directedGraph);

		// Create all threads
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 1; i < REPLICATION_FACTOR; i++) {
			Thread t = new Thread(runnable);
			t.setName(String.valueOf(i));
			threads.add(t);
		}
//		Thread t2 = new Thread(runnable);
//		Thread t3 = new Thread(runnable);
//		Thread t4 = new Thread(runnable);
//		Thread t5 = new Thread(runnable);
//		Thread t6 = new Thread(runnable);

//		t2.setName("1");
//		t3.setName("2");
//		t4.setName("3");
//		t5.setName("4");
//		t6.setName("5");

		// Start all threads
		for (Thread thread : threads) {
			thread.start();
		}

//		t2.start();
//		t3.start();
//		t4.start();
//		t5.start();
//		t6.start();

		// Wait for all threads to join
		for (Thread thread : threads) {
			thread.join();
		}
//		t2.join();
//		t3.join();
//		t4.join();
//		t5.join();
//		t6.join();

		System.out.println("Data after copying");
		for (Node tmpNode : nodes) {
			System.out.println(tmpNode.toString());
		}

		//System.out.println("Time after program start : " + Util.getDate());
	}


    String bpid = cluster.getNamesystem().getBlockPoolId();
    for (DataNode dn : cluster.getDataNodes()) {
      Replica r = DataNodeTestUtils.fetchReplicaInfo(dn, bpid, lb.get(0)
          .getBlock().getBlockId());

      assertTrue("Replica on DN " + dn + " shouldn't be null", r != null);
      assertEquals("Should be RBW replica on " + dn
          + " after sequence of calls append()/write()/hflush()",
          HdfsServerConstants.ReplicaState.RBW, r.getState());
    }
    ofs.close();
  }

  /**
   * These two test cases are already implemented by
   *
   * @link{TestReadWhileWriting}
   */
  public void pipeline_02_03() {
  }
  
  static byte[] writeData(final FSDataOutputStream out, final int length)
    throws IOException {
    int bytesToWrite = length;
    byte[] ret = new byte[bytesToWrite];
    byte[] toWrite = new byte[1024];
    int written = 0;
    Random rb = new Random(rand.nextLong());
    while (bytesToWrite > 0) {
      rb.nextBytes(toWrite);
      int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : bytesToWrite;
      out.write(toWrite, 0, bytesToWriteNext);
      System.arraycopy(toWrite, 0, ret, (ret.length - bytesToWrite),
        bytesToWriteNext);
      written += bytesToWriteNext;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Written: " + bytesToWriteNext + "; Total: " + written);
      }
      bytesToWrite -= bytesToWriteNext;
    }
    return ret;
  }
  
  private static void setConfiguration() {
    conf = new Configuration();
    int customPerChecksumSize = 700;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, customBlockSize / 2);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 0);
  }

  private static void initLoggers() {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }
}
