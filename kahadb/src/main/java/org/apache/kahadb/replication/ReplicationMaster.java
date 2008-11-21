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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.Service;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.Callback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.DataFile;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.journal.ReplicationTarget;
import org.apache.kahadb.replication.pb.PBFileInfo;
import org.apache.kahadb.replication.pb.PBHeader;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBJournalUpdate;
import org.apache.kahadb.replication.pb.PBSlaveInit;
import org.apache.kahadb.replication.pb.PBSlaveInitResponse;
import org.apache.kahadb.replication.pb.PBType;
import org.apache.kahadb.store.KahaDBStore;
import org.apache.kahadb.util.ByteSequence;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

public class ReplicationMaster implements Service, ClusterListener, ReplicationTarget {

	private static final Log LOG = LogFactory.getLog(ReplicationService.class);

	private final ReplicationService replicationService;

	private Object serverMutex = new Object() {};
	private TransportServer server;
	
	private ArrayList<ReplicationSession> sessions = new ArrayList<ReplicationSession>();
	
	private final AtomicInteger nextSnapshotId = new AtomicInteger();
    private final Map<Location, CountDownLatch> requestMap = new LinkedHashMap<Location, CountDownLatch>();

	public ReplicationMaster(ReplicationService replicationService) {
		this.replicationService = replicationService;
	}

	public void start() throws Exception {
		synchronized (serverMutex) {
			server = TransportFactory.bind(new URI(replicationService.getUri()));
			server.setAcceptListener(new TransportAcceptListener() {
				public void onAccept(Transport transport) {
					try {
						synchronized (serverMutex) {
							ReplicationSession session = new ReplicationSession(transport);
							session.start();
							addSession(session);
						}
					} catch (Exception e) {
						LOG.info("Could not accept replication connection from slave at " + transport.getRemoteAddress() + ", due to: " + e, e);
					}
				}

				public void onAcceptError(Exception e) {
					LOG.info("Could not accept replication connection: " + e, e);
				}
			});
			server.start();
		}
		replicationService.getStore().getJournal().setReplicationTarget(this);
	}
	
    boolean isStarted() {
        synchronized (serverMutex) {
            return server!=null;
        }
    }
    
    public void stop() throws Exception {
        replicationService.getStore().getJournal().setReplicationTarget(null);
        synchronized (serverMutex) {
            if (server != null) {
                server.stop();
                server = null;
            }
        }
        
        ArrayList<ReplicationSession> sessionsSnapshot;
        synchronized (this.sessions) {
            sessionsSnapshot = this.sessions;
        }
        
        for (ReplicationSession session: sessionsSnapshot) {
            session.stop();
        }
    }

	protected void addSession(ReplicationSession session) {
	    synchronized (sessions) {
	        sessions = new ArrayList<ReplicationSession>(sessions);
	        sessions.add(session);
        }
    }
	
    protected void removeSession(ReplicationSession session) {
        synchronized (sessions) {
            sessions = new ArrayList<ReplicationSession>(sessions);
            sessions.remove(session);
        }
    }

	public void onClusterChange(ClusterState config) {
		// For now, we don't really care about changes in the slave config..
	}

	/**
	 * This is called by the Journal so that we can replicate the update to the 
	 * slaves.
	 */
	public void replicate(Location location, ByteSequence sequence, boolean sync) {
	    ArrayList<ReplicationSession> sessionsSnapshot;
        synchronized (this.sessions) {
            // Hurrah for copy on write..
            sessionsSnapshot = this.sessions;
        }
	    

        // We may be configured to always do async replication..
		if ( replicationService.isAsyncReplication() ) {
		    sync=false;
		}
		CountDownLatch latch=null;
		if( sync ) {
    		latch = new CountDownLatch(1);
            synchronized (requestMap) {
                requestMap.put(location, latch);
            }
		}
		
		ReplicationFrame frame=null;
		for (ReplicationSession session : sessionsSnapshot) {
			if( session.subscribedToJournalUpdates.get() ) {
			    
			    // Lazy create the frame since we may have not avilable sessions to send to.
			    if( frame == null ) {
    		        frame = new ReplicationFrame();
                    frame.setHeader(new PBHeader().setType(PBType.JOURNAL_UPDATE));
                    PBJournalUpdate payload = new PBJournalUpdate();
                    payload.setLocation(ReplicationSupport.convert(location));
                    payload.setData(new org.apache.activemq.protobuf.Buffer(sequence.getData(), sequence.getOffset(), sequence.getLength()));
                    payload.setSendAck(sync);
                    frame.setPayload(payload);
			    }

				// TODO: use async send threads so that the frames can be pushed out in parallel. 
				try {
				    session.setLastUpdateLocation(location);
					session.transport.oneway(frame);
				} catch (IOException e) {
					session.onException(e);
				}
			}
		}
		
        if (sync) {
            try {
                int timeout = 500;
                int counter=0;
                while( true ) {
                    if( latch.await(timeout, TimeUnit.MILLISECONDS) ) {
                        synchronized (requestMap) {
                            requestMap.remove(location);
                        }
                        return;
                    }
                    if( !isStarted() ) {
                        return;
                    }
                    counter++;
                    if( (counter%10)==0 ) {
                        LOG.warn("KahaDB is waiting for slave to come online. "+(timeout*counter/1000.f)+" seconds have elapsed.");
                    }
                } 
            } catch (InterruptedException ignore) {
            }
        }
		
	}
	
    private void ackAllFromTo(Location lastAck, Location newAck) {
        if ( replicationService.isAsyncReplication() ) {
            return;
        }
        
        ArrayList<Entry<Location, CountDownLatch>> entries;
        synchronized (requestMap) {
            entries = new ArrayList<Entry<Location, CountDownLatch>>(requestMap.entrySet());
        }
        boolean inRange=false;
        for (Entry<Location, CountDownLatch> entry : entries) {
            Location l = entry.getKey();
            if( !inRange ) {
                if( lastAck==null || lastAck.compareTo(l) < 0 ) {
                    inRange=true;
                }
            }
            if( inRange ) {
                entry.getValue().countDown();
                if( newAck!=null && l.compareTo(newAck) <= 0 ) {
                    return;
                }
            }
        }
    }


	class ReplicationSession implements Service, TransportListener {

		private final Transport transport;
		private final AtomicBoolean subscribedToJournalUpdates = new AtomicBoolean();
        private boolean stopped;
		
		private File snapshotFile;
		private HashSet<Integer> journalReplicatedFiles;
		private Location lastAckLocation;
        private Location lastUpdateLocation;
        private boolean online;

		public ReplicationSession(Transport transport) {
			this.transport = transport;
		}

		synchronized public void setLastUpdateLocation(Location lastUpdateLocation) {
            this.lastUpdateLocation = lastUpdateLocation;
        }

        public void start() throws Exception {
			transport.setTransportListener(this);
			transport.start();
		}

        synchronized public void stop() throws Exception {
		    if ( !stopped  ) { 
		        stopped=true;
    			deleteReplicationData();
    			transport.stop();
		    }
		}

		synchronized private void onJournalUpdateAck(ReplicationFrame frame, PBJournalLocation location) {
            Location ack = ReplicationSupport.convert(location);
		    if( online ) {
                ackAllFromTo(lastAckLocation, ack);
		    }
            lastAckLocation=ack;
	    }
		
		synchronized private void onSlaveOnline(ReplicationFrame frame) {
            deleteReplicationData();
            online  = true;
            if( lastAckLocation!=null ) {
                ackAllFromTo(null, lastAckLocation);
            }
            
        }

        public void onCommand(Object command) {
			try {
				ReplicationFrame frame = (ReplicationFrame) command;
				switch (frame.getHeader().getType()) {
				case SLAVE_INIT:
					onSlaveInit(frame, (PBSlaveInit) frame.getPayload());
					break;
				case SLAVE_ONLINE:
					onSlaveOnline(frame);
					break;
				case FILE_TRANSFER:
					onFileTransfer(frame, (PBFileInfo) frame.getPayload());
					break;
				case JOURNAL_UPDATE_ACK:
					onJournalUpdateAck(frame, (PBJournalLocation) frame.getPayload());
					break;
				}
			} catch (Exception e) {
				LOG.warn("Slave request failed: "+e, e);
				failed(e);
			}
		}

		public void onException(IOException error) {
			failed(error);
		}

		public void failed(Exception error) {
			try {
				stop();
			} catch (Exception ignore) {
			}
		}

		public void transportInterupted() {
		}
		public void transportResumed() {
		}
		
		private void deleteReplicationData() {
			if( snapshotFile!=null ) {
				snapshotFile.delete();
				snapshotFile=null;
			}
			if( journalReplicatedFiles!=null ) {
				journalReplicatedFiles=null;
				updateJournalReplicatedFiles();
			}
		}

		private void onSlaveInit(ReplicationFrame frame, PBSlaveInit slaveInit) throws Exception {

			// Start sending journal updates to the slave.
			subscribedToJournalUpdates.set(true);

			// We could look at the slave state sent in the slaveInit and decide
			// that a full sync is not needed..
			// but for now we will do a full sync every time.
			ReplicationFrame rc = new ReplicationFrame();
			final PBSlaveInitResponse rcPayload = new PBSlaveInitResponse();
			rc.setHeader(new PBHeader().setType(PBType.SLAVE_INIT_RESPONSE));
			rc.setPayload(rcPayload);
			
			// Setup a map of all the files that the slave has
			final HashMap<String, PBFileInfo> slaveFiles = new HashMap<String, PBFileInfo>();
			for (PBFileInfo info : slaveInit.getCurrentFilesList()) {
				slaveFiles.put(info.getName(), info);
			}
			
			
			final KahaDBStore store = replicationService.getStore();
			store.checkpoint(new Callback() {
				public void execute() throws Exception {
					// This call back is executed once the checkpoint is
					// completed and all data has been synced to disk, 
					// but while a lock is still held on the store so 
					// that no updates are done while we are in this
					// method.
					
					KahaDBStore store = replicationService.getStore();
					if( lastAckLocation==null ) {
					    lastAckLocation = store.getLastUpdatePosition();
					}
					
					int snapshotId = nextSnapshotId.incrementAndGet();
					File file = store.getPageFile().getFile();
					File dir = replicationService.getTempReplicationDir();
					dir.mkdirs();
					snapshotFile = new File(dir, "snapshot-" + snapshotId);
					
					journalReplicatedFiles = new HashSet<Integer>();
					
					// Store the list files associated with the snapshot.
					ArrayList<PBFileInfo> snapshotInfos = new ArrayList<PBFileInfo>();
					Map<Integer, DataFile> journalFiles = store.getJournal().getFileMap();
					for (DataFile df : journalFiles.values()) {
						// Look at what the slave has so that only the missing bits are transfered.
						String name = "journal-" + df.getDataFileId();
						PBFileInfo slaveInfo = slaveFiles.remove(name);
						
						// Use the checksum info to see if the slave has the file already.. Checksums are less acurrate for
						// small amounts of data.. so ignore small files.
						if( slaveInfo!=null && slaveInfo.getEnd()> 1024*512 ) {
							// If the slave's file checksum matches what we have..
							if( ReplicationSupport.checksum(df.getFile(), 0, slaveInfo.getEnd())==slaveInfo.getChecksum() ) {
								// is Our file longer? then we need to continue transferring the rest of the file.
								if( df.getLength() > slaveInfo.getEnd() ) {
									snapshotInfos.add(ReplicationSupport.createInfo(name, df.getFile(), slaveInfo.getEnd(), df.getLength()));
									journalReplicatedFiles.add(df.getDataFileId());
									continue;
								} else {
									// No need to replicate this file.
									continue;
								}
							} 
						}
						
						// If we got here then it means we need to transfer the whole file.
						snapshotInfos.add(ReplicationSupport.createInfo(name, df.getFile(), 0, df.getLength()));
						journalReplicatedFiles.add(df.getDataFileId());
					}

					PBFileInfo info = new PBFileInfo();
					info.setName("database");
					info.setSnapshotId(snapshotId);
					info.setStart(0);
					info.setEnd(file.length());
					info.setChecksum(ReplicationSupport.copyAndChecksum(file, snapshotFile));
					snapshotInfos.add(info);
					
					rcPayload.setCopyFilesList(snapshotInfos);
					ArrayList<String> deleteFiles = new ArrayList<String>();
					slaveFiles.remove("database");
					for (PBFileInfo unused : slaveFiles.values()) {
						deleteFiles.add(unused.getName());
					}
					rcPayload.setDeleteFilesList(deleteFiles);
					
					updateJournalReplicatedFiles();
				}

			});
			
			transport.oneway(rc);
		}
		
		private void onFileTransfer(ReplicationFrame frame, PBFileInfo fileInfo) throws IOException {
			File file = replicationService.getReplicationFile(fileInfo.getName());
			long payloadSize = fileInfo.getEnd()-fileInfo.getStart();
			
			if( file.length() < fileInfo.getStart()+payloadSize ) {
				throw new IOException("Requested replication file dose not have enough data.");
			}		
			
			ReplicationFrame rc = new ReplicationFrame();
			rc.setHeader(new PBHeader().setType(PBType.FILE_TRANSFER_RESPONSE).setPayloadSize(payloadSize));
			
			FileInputStream is = new FileInputStream(file);
			rc.setPayload(is);
			try {
				is.skip(fileInfo.getStart());
				transport.oneway(rc);
			} finally {
				try {
					is.close();
				} catch (Throwable e) {
				}
			}
		}

	}

	/**
	 * Looks at all the journal files being currently replicated and informs the KahaDB so that
	 * it does not delete them while the replication is occuring.
	 */
	private void updateJournalReplicatedFiles() {
		HashSet<Integer>  files = replicationService.getStore().getJournalFilesBeingReplicated();
		files.clear();

        ArrayList<ReplicationSession> sessionsSnapshot;
        synchronized (this.sessions) {
            // Hurrah for copy on write..
            sessionsSnapshot = this.sessions;
        }
        
		for (ReplicationSession session : sessionsSnapshot) {
			if( session.journalReplicatedFiles !=null ) {
				files.addAll(session.journalReplicatedFiles);
			}
		}
	}
	
}
