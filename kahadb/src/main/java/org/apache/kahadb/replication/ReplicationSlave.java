/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kahadb.replication;

import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.Service;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.DataFile;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.replication.pb.PBFileInfo;
import org.apache.kahadb.replication.pb.PBHeader;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBJournalUpdate;
import org.apache.kahadb.replication.pb.PBSlaveInit;
import org.apache.kahadb.replication.pb.PBSlaveInitResponse;
import org.apache.kahadb.replication.pb.PBType;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus.State;
import org.apache.kahadb.store.KahaDBStore;

public class ReplicationSlave implements Service, ClusterListener, TransportListener {
	
	private static final int MAX_TRANSFER_SESSIONS = 1;

	private static final Log LOG = LogFactory.getLog(ReplicationSlave.class);

	private final ReplicationService replicationServer;
	private Transport transport;

	// Used to bulk transfer the master state over to the slave..
	private final Object transferMutex = new Object();
	private final LinkedList<PBFileInfo> transferQueue = new LinkedList<PBFileInfo>();
	private final LinkedList<TransferSession> transferSessions = new LinkedList<TransferSession>();
	private final HashMap<String, PBFileInfo> bulkFiles = new HashMap<String, PBFileInfo>();	
	private PBSlaveInitResponse initResponse;
	private boolean online;
	private final AtomicBoolean started = new AtomicBoolean();
	
	// Used to do real time journal updates..
	int journalUpdateFileId;
	RandomAccessFile journalUpateFile;
	private String master;
	
	public ReplicationSlave(ReplicationService replicationServer) {
		this.replicationServer = replicationServer;
	}

	public void start() throws Exception {
		if( started.compareAndSet(false, true)) {
	        onClusterChange(replicationServer.getClusterState());

		}
	}
	
	public void stop() throws Exception {
		if( started.compareAndSet(true, false)) {
			doStop();
		}
	}

	private void doStart() throws Exception, URISyntaxException, IOException {
		synchronized (transferMutex) {
			
			// Failure recovery might be trying to start us back up,
			// but the Replication server may have already stopped us so there is not need to start up.
			if( !started.get() ) {
				return;
			}
			
			replicationServer.getCluster().setMemberStatus(replicationServer.createStatus(State.SLAVE_SYNCRONIZING));
			
			transport = TransportFactory.connect(new URI(master));
			transport.setTransportListener(this);
			transport.start();
	
			// Make sure the replication directory exists.
			replicationServer.getTempReplicationDir().mkdirs();
			
			ReplicationFrame frame = new ReplicationFrame();
			frame.setHeader(new PBHeader().setType(PBType.SLAVE_INIT));
			PBSlaveInit payload = new PBSlaveInit();
			payload.setNodeId(replicationServer.getUri());
			
			// This call back is executed once the checkpoint is
			// completed and all data has been
			// synced to disk, but while a lock is still held on the
			// store so that no
			// updates are allowed.
	
			HashMap<String, PBFileInfo> infosMap = new HashMap<String, PBFileInfo>();
			
			// Add all the files that were being transfered..
			File tempReplicationDir = replicationServer.getTempReplicationDir();
			File[] list = tempReplicationDir.listFiles();
			if( list!=null ) {
				for (File file : list) {
					String name = file.getName();
					if( name.startsWith("database-") ) {
						int snapshot;
						try {
							snapshot = Integer.parseInt(name.substring("database-".length()));
						} catch (NumberFormatException e) {
							continue;
						}
						
						PBFileInfo info = ReplicationSupport.createInfo("database", file, 0, file.length());
						info.setSnapshotId(snapshot);
						infosMap.put("database", info);
					} else if( name.startsWith("journal-") ) {
						PBFileInfo info = ReplicationSupport.createInfo(name, file, 0, file.length());
						infosMap.put(name, info);
					}
				}
			}
			
			// Add all the db files that were not getting transfered..
			KahaDBStore store = replicationServer.getStore();
			Map<Integer, DataFile> journalFiles = store.getJournal().getFileMap();
			for (DataFile df : journalFiles.values()) {
				String name = "journal-" + df.getDataFileId();
				// Did we have a transfer in progress for that file already?
				if( infosMap.containsKey(name) ) {
					continue;
				}
				infosMap.put(name, ReplicationSupport.createInfo(name, df.getFile(), 0, df.getLength()));
			}
			if( !infosMap.containsKey("database") ) {
				File pageFile = store.getPageFile().getFile();
				if( pageFile.exists() ) {
					infosMap.put("database", ReplicationSupport.createInfo("database", pageFile, 0, pageFile.length()));
				}
			}
			
			ArrayList<PBFileInfo> infos = new ArrayList<PBFileInfo>(infosMap.size());
			for (PBFileInfo info : infosMap.values()) {
				infos.add(info);
			}
			payload.setCurrentFilesList(infos);
			
			frame.setPayload(payload);
			LOG.info("Sending master slave init command: " + payload);
			online = false;
			transport.oneway(frame);
		}
	}

	private void doStop() throws Exception, IOException {
		synchronized (transferMutex) {
			if( this.transport!=null ) {
				this.transport.stop();
				this.transport=null;
			}
	
			// Stop any current transfer sessions.
			for (TransferSession session : this.transferSessions) {
				session.stop();
			}
	
			this.transferQueue.clear();
			
			this.initResponse=null;
			this.bulkFiles.clear();	
			this.online=false;
	
			if( journalUpateFile !=null ) {
				journalUpateFile.close();
				journalUpateFile=null;
			}
			journalUpdateFileId=0;
		}
	}

	public void onClusterChange(ClusterState config) {
		synchronized (transferMutex) {
			try {
	            if( master==null || !master.equals(config.getMaster()) ) {
                    master = config.getMaster();
		            doStop();
				    doStart();
	            }
			} catch (Exception e) {
				LOG.error("Could not restart syncing with new master: "+config.getMaster()+", due to: "+e,e);
			}
		}
	}

	public void onCommand(Object command) {
		try {
			ReplicationFrame frame = (ReplicationFrame) command;
			switch (frame.getHeader().getType()) {
			case SLAVE_INIT_RESPONSE:
				onSlaveInitResponse(frame, (PBSlaveInitResponse) frame.getPayload());
				break;
			case JOURNAL_UPDATE:
				onJournalUpdate(frame, (PBJournalUpdate) frame.getPayload());
			}
		} catch (Exception e) {
			failed(e);
		}
	}

	public void onException(IOException error) {
		failed(error);
	}

	public void failed(Throwable error) {
		try {
			if( started.get() ) {
				LOG.warn("Replication session fail to master: "+transport.getRemoteAddress(), error);
				doStop();
				// Wait a little an try to establish the session again..
				Thread.sleep(1000);
				doStart();
			}
		} catch (Exception ignore) {
		}
	}

	public void transportInterupted() {
	}
	public void transportResumed() {
	}
	
	private void onJournalUpdate(ReplicationFrame frame, PBJournalUpdate update) throws IOException {
	    
	    // Send an ack back once we get the ack.. yeah it's a little dirty to ack before it's on disk,
	    // but chances are low that both machines are going to loose power at the same time and this way,
	    // we reduce the latency the master sees from us.
	    if( update.getSendAck() ) {
	        ReplicationFrame ack = new ReplicationFrame();
	        ack.setHeader(new PBHeader().setType(PBType.JOURNAL_UPDATE_ACK));
	        ack.setPayload(update.getLocation());
	        transport.oneway(ack);
	    }
	    
	    // TODO: actually do the disk write in an async thread so that this thread can be  
	    // start reading in the next journal updated.
	    
		boolean onlineRecovery=false;
		PBJournalLocation location = update.getLocation();
		byte[] data = update.getData().toByteArray();
		synchronized (transferMutex) {
			if( journalUpateFile==null || journalUpdateFileId!=location.getFileId() ) {
				if( journalUpateFile!=null) {
					journalUpateFile.close();
				}
				File file;
				String name = "journal-"+location.getFileId();
				if( !online ) {
					file = replicationServer.getTempReplicationFile(name, 0);
					if( !bulkFiles.containsKey(name) ) {
						bulkFiles.put(name, new PBFileInfo().setName(name));
					}
				} else {
					// Once the data has been synced.. we are going to 
					// go into an online recovery mode...
					file = replicationServer.getReplicationFile(name);
				}
				journalUpateFile = new RandomAccessFile(file, "rw");
				journalUpdateFileId = location.getFileId();
			}
			
//			System.out.println("Writing: "+location.getFileId()+":"+location.getOffset()+" with "+data.length);
			journalUpateFile.seek(location.getOffset());
			journalUpateFile.write(data);
			if( online ) {
				onlineRecovery=true;
			}
		}
		
		if( onlineRecovery ) {
			KahaDBStore store = replicationServer.getStore();
			// Let the journal know that we appended to one of it's files..
			store.getJournal().appendedExternally(ReplicationSupport.convert(location), data.length);
			// Now incrementally recover those records.
			store.incrementalRecover();
		}
	}
	
	
	private void commitBulkTransfer() {
		try {
			
			synchronized (transferMutex) {
				
				LOG.info("Slave synhcronization complete, going online...");
				replicationServer.getStore().close();
				
				if( journalUpateFile!=null ) {
					journalUpateFile.close();
					journalUpateFile=null;
				}
				
				// If we got a new snapshot of the database, then we need to 
				// delete it's assisting files too.
				if( bulkFiles.containsKey("database") ) {
					PageFile pageFile = replicationServer.getStore().getPageFile();
					pageFile.getRecoveryFile().delete();
					pageFile.getFreeFile().delete();
				}
				
				for (PBFileInfo info : bulkFiles.values()) {
					File from = replicationServer.getTempReplicationFile(info.getName(), info.getSnapshotId());
					File to = replicationServer.getReplicationFile(info.getName());
					to.getParentFile().mkdirs();
					move(from, to);
				}
				
				delete(initResponse.getDeleteFilesList());
				online=true;
				
				replicationServer.getStore().open();
				
	            replicationServer.getCluster().setMemberStatus(replicationServer.createStatus(State.SLAVE_ONLINE));
				LOG.info("Slave is now online.  We are now eligible to become the master.");
			}
			
			
			
			// Let the master know we are now online.
			ReplicationFrame frame = new ReplicationFrame();
			frame.setHeader(new PBHeader().setType(PBType.SLAVE_ONLINE));
			transport.oneway(frame);
			
		} catch (Throwable e) {
			e.printStackTrace();
			failed(e);
		}
	}

	private void onSlaveInitResponse(ReplicationFrame frame, PBSlaveInitResponse response) throws Exception {
		LOG.info("Got init response: " + response);
		initResponse = response;
		
		synchronized (transferMutex) {
			bulkFiles.clear();
			
			List<PBFileInfo> infos = response.getCopyFilesList();
			for (PBFileInfo info : infos) {
				
				bulkFiles.put(info.getName(), info);
				File target = replicationServer.getReplicationFile(info.getName());
				// are we just appending to an existing file journal file?
				if( info.getName().startsWith("journal-") && info.getStart() > 0 && target.exists() ) {
					// Then copy across the first bits..
					File tempFile = replicationServer.getTempReplicationFile(info.getName(), info.getSnapshotId());
					
					FileInputStream is = new FileInputStream(target);
					FileOutputStream os = new FileOutputStream(tempFile);
					try {
						copy(is, os, info.getStart());
					} finally {
						try { is.close(); } catch (Throwable e){}
						try { os.close(); } catch (Throwable e){}
					}
				}
			}
			
			
			transferQueue.clear();
			transferQueue.addAll(infos);
		}
		addTransferSession();
	}

	private PBFileInfo dequeueTransferQueue() throws Exception {
		synchronized (transferMutex) {
			if (transferQueue.isEmpty()) {
				return null;
			}
			return transferQueue.removeFirst();
		}
	}

	private void addTransferSession() {
		synchronized (transferMutex) {
			while (transport!=null && !transferQueue.isEmpty() && transferSessions.size() < MAX_TRANSFER_SESSIONS) {
				TransferSession transferSession = new TransferSession();
				transferSessions.add(transferSession);
				try {
					transferSession.start();
				} catch (Exception e) {
					transferSessions.remove(transferSession);
				}
			}
			// Once we are done processing all the transfers..
			if (transferQueue.isEmpty() && transferSessions.isEmpty()) {
				commitBulkTransfer();
			}
		}
	}

	private void move(File from, File to) throws IOException {
		
		// If a simple rename/mv does not work..
		to.delete();
		if (!from.renameTo(to)) {
			
			// Copy and Delete.
			FileInputStream is = null;
			FileOutputStream os = null;
			try {
				is = new FileInputStream(from);
				os = new FileOutputStream(to);

				os.getChannel().transferFrom(is.getChannel(), 0, is.getChannel().size());
			} finally {
				try {
					is.close();
				} catch(Throwable e) {
				}
				try {
					os.close();
				} catch(Throwable e) {
				}
			}
			from.delete();
		}
	}

	class TransferSession implements Service, TransportListener {

		Transport transport;
		private PBFileInfo info;
		private File toFile;
		private AtomicBoolean stopped = new AtomicBoolean();
		private long transferStart;

		public void start() throws Exception {
			LOG.info("File transfer session started.");
			transport = TransportFactory.connect(new URI(replicationServer.getClusterState().getMaster()));
			transport.setTransportListener(this);
			transport.start();
			sendNextRequestOrStop();
		}

		private void sendNextRequestOrStop() {
			try {
				PBFileInfo info = dequeueTransferQueue();
				if (info != null) {

					toFile = replicationServer.getTempReplicationFile(info.getName(), info.getSnapshotId());
					this.info = info;

					ReplicationFrame frame = new ReplicationFrame();
					frame.setHeader(new PBHeader().setType(PBType.FILE_TRANSFER));
					frame.setPayload(info);

					LOG.info("Requesting file: " + info.getName());
					transferStart = System.currentTimeMillis();

					transport.oneway(frame);
				} else {
					stop();
				}

			} catch (Exception e) {
				failed(e);
			}
		}

		public void stop() throws Exception {
			if (stopped.compareAndSet(false, true)) {
				LOG.info("File transfer session stopped.");
				synchronized (transferMutex) {
					if (info != null) {
						transferQueue.addLast(info);
					}
					info = null;
				}
				transport.stop();
				synchronized (transferMutex) {
					transferSessions.remove(TransferSession.this);
					addTransferSession();
				}
			}
		}

		public void onCommand(Object command) {
			try {
				ReplicationFrame frame = (ReplicationFrame) command;
				InputStream is = (InputStream) frame.getPayload();
				toFile.getParentFile().mkdirs();
				
				RandomAccessFile os = new RandomAccessFile(toFile, "rw");
				os.seek(info.getStart());
				try {
					copy(is, os, frame.getHeader().getPayloadSize());
					long transferTime = System.currentTimeMillis() - this.transferStart;
					float rate = frame.getHeader().getPayloadSize() * transferTime / 1024000f;
					LOG.info("File " + info.getName() + " transfered in " + transferTime + " (ms) at " + rate + " Kb/Sec");
				} finally {
					os.close();
				}
				this.info = null;
				this.toFile = null;

				sendNextRequestOrStop();
			} catch (Exception e) {
				failed(e);
			}
		}

		public void onException(IOException error) {
			failed(error);
		}

		public void failed(Exception error) {
			try {
				if (!stopped.get()) {
					LOG.warn("Replication session failure: " + transport.getRemoteAddress());
				}
				stop();
			} catch (Exception ignore) {
			}
		}

		public void transportInterupted() {
		}

		public void transportResumed() {
		}

	}

	private void copy(InputStream is, OutputStream os, long length) throws IOException {
		byte buffer[] = new byte[1024 * 4];
		int c = 0;
		long pos = 0;
		while (pos < length && ((c = is.read(buffer, 0, (int) Math.min(buffer.length, length - pos))) >= 0)) {
			os.write(buffer, 0, c);
			pos += c;
		}
	}
	
	private void copy(InputStream is, DataOutput os, long length) throws IOException {
		byte buffer[] = new byte[1024 * 4];
		int c = 0;
		long pos = 0;
		while (pos < length && ((c = is.read(buffer, 0, (int) Math.min(buffer.length, length - pos))) >= 0)) {
			os.write(buffer, 0, c);
			pos += c;
		}
	}
	
	private void delete(List<String> files) {
		for (String fn : files) {
			try {
				replicationServer.getReplicationFile(fn).delete();
			} catch (IOException e) {
			}
		}
	}

}
