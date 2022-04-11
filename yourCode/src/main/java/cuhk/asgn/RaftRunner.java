package cuhk.asgn;

import cuhk.asgn.runnable.*;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import raft.Raft;
import raft.Raft.AppendEntriesArgs;
import raft.Raft.AppendEntriesReply;
import raft.Raft.CheckEventsArgs;
import raft.Raft.CheckEventsReply;
import raft.Raft.GetValueArgs;
import raft.Raft.GetValueReply;
import raft.Raft.LogEntry;
import raft.Raft.ProposeArgs;
import raft.Raft.ProposeReply;
import raft.Raft.RequestVoteArgs;
import raft.Raft.RequestVoteReply;
import raft.Raft.SetElectionTimeoutArgs;
import raft.Raft.SetElectionTimeoutReply;
import raft.Raft.SetHeartBeatIntervalArgs;
import raft.Raft.SetHeartBeatIntervalReply;
import raft.RaftNodeGrpc;
import raft.RaftNodeGrpc.RaftNodeBlockingStub;

public class RaftRunner {
    public static void main(String[] args) throws Exception {
        String ports = args[1];
        int myport = Integer.parseInt(args[0]);
        int nodeID = Integer.parseInt(args[2]);
        int heartBeatInterval = Integer.parseInt(args[3]);
        int electionTimeout = Integer.parseInt(args[4]);
        String[] portStrings = ports.split(",");

        // A map where
        // 		the key is the node id
        //		the value is the {hostname:port}
        Map<Integer, Integer> hostmap = new HashMap<>();
        for (int x = 0; x < portStrings.length; x++) {
            hostmap.put(x, Integer.valueOf(portStrings[x]));
        }

        RaftNode node = NewRaftNode(myport, hostmap, nodeID, heartBeatInterval, electionTimeout);

        final Server server = node.getGrpcServer();
        //Stop the server
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                server.shutdown();
                System.err.println("*** server shut down");
            }
        });
        server.awaitTermination();
    }

    // Desc:
    // NewRaftNode creates a new RaftNode. This function should return only when
    // all nodes have joined the ring, and should return a non-nil error if this node
    // could not be started in spite of dialing any other nodes.
    //
    // Params:
    // myport: the port of this new node. We use tcp in this project.
    //			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
    // nodeidPortMap: a map from all node IDs to their ports.
    // nodeId: the id of this node
    // heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
    // electionTimeout: The election timeout for this node. In millisecond.
    public static RaftNode NewRaftNode(int myPort, Map<Integer, Integer> nodeidPortMap, int nodeId, int heartBeatInterval,
                                       int electionTimeout) throws IOException {
        //TODO: implement this !

//        nodeidPortMap.remove(nodeId);
        Variables.electionTimeout = electionTimeout;
        Variables.heartBeatInterval = heartBeatInterval;
        Map<Integer, RaftNodeBlockingStub> hostConnectionMap = new HashMap<>();
        RaftNode raftNode = new RaftNode();
        Server server = ServerBuilder.forPort(myPort).addService(raftNode).build();
        raftNode.server = server;
        raftNode.state.nodeId = nodeId;
        server.start();
        //crate channel to other RaftNode
        for (Map.Entry<Integer, Integer> entry : nodeidPortMap.entrySet()) {
            int id = entry.getValue();
            Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1", id)
                    .usePlaintext() // disable TLS
                    .build();
            hostConnectionMap.put(
                    entry.getKey(),
                    RaftNodeGrpc.newBlockingStub(channel)
            );
        }
        raftNode.state.hostConnectionMap = hostConnectionMap;
        System.out.println("Successfully connect all nodes");
        //TODO: kick off leader election here !
        raftNode.taskHolder = new TaskHolder(raftNode.state, raftNode);
        return raftNode;
    }


    public static class RaftNode extends RaftNodeGrpc.RaftNodeImplBase {
        public Server server;
        //use volatile keyword
        public volatile State state;
        public TaskHolder taskHolder;
        ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap();

        public RaftNode() {
            this.state = new State();
        }

        public void leaderInit() {
            int size = state.hostConnectionMap.size();
            state.nextIndex = new int[size];
            state.matchIndex = new int[size];
            taskHolder.stopElection();
            taskHolder.addHeartBeat();
            int curIndex = state.log.size() + 1;
            for (int i = 0; i < size; i++) {
                state.nextIndex[i] = curIndex;
                state.matchIndex[i] = 0;
            }
            //
        }

        // Desc:
        // Propose initializes proposing a new operation, and replies with the
        // result of committing this operation. Propose should not return until
        // this operation has been committed, or this node is not leader now.
        //
        // If the we put a new <k, v> pair or deleted an existing <k, v> pair
        // successfully, it should return OK; If it tries to delete an non-existing
        // key, a KeyNotFound should be returned; If this node is not leader now,
        // it should return WrongNode as well as the currentLeader id.
        //
        // Params:
        // args: the operation to propose
        // reply: as specified in Desc
        @Override
        public void propose(ProposeArgs request, StreamObserver<ProposeReply> responseObserver) {
            ProposeReply reply = null;
            System.out.println("get propose");
            //if not leader,return wrong node
            if (!state.role.equals(Raft.Role.Leader)) {
                reply = ProposeReply.newBuilder().setStatus(Raft.Status.WrongNode).setCurrentLeader(state.leaderId).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            Raft.Operation operation = request.getOp();
            //current node is leader
            String key = request.getKey();
            int value = request.getV();
            switch (operation) {
                case UNRECOGNIZED:
                    reply = ProposeReply.newBuilder().setStatus(Raft.Status.UNRECOGNIZED).setCurrentLeader(state.getLeaderId()).build();
                    break;
                case Put:
                    System.out.println("receive put");
                    concurrentHashMap.put(key, value);
                    LogEntry logEntry = appendLocalEntry(request);
                    try {
                        if (taskHolder.waitUntilMajority(logEntry, request)) {
                            leaderRefresh();
                        }
                        ;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    state.commitIndex.getAndIncrement();
                    reply = ProposeReply.newBuilder().setStatus(Raft.Status.OK).setCurrentLeader(state.getLeaderId()).build();
                    break;
                case Delete:
                    System.out.println("receive delete");
                    if (concurrentHashMap.get(key) == null) {
                        reply = ProposeReply.newBuilder().setCurrentLeader(state.getLeaderId()).setStatus(Raft.Status.KeyNotFound).build();
                    } else {
                        logEntry = appendLocalEntry(request);
                        try {
                            if (taskHolder.waitUntilMajority(logEntry, request)) {
                                leaderRefresh();
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        reply = ProposeReply.newBuilder().setStatus(Raft.Status.OK).setCurrentLeader(state.getLeaderId()).build();
                    }
                    break;
            }
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }


        // Desc:GetValue
        // GetValue looks up the value for a key, and replies with the value or with
        // the Status KeyNotFound.
        //
        // Params:
        // args: the key to check
        // reply: the value and status for this lookup of the given key
        @Override
        public void getValue(GetValueArgs request, StreamObserver<GetValueReply> responseObserver) {
            // TODO: Implement this!
            System.out.println("node" + state.nodeId + "receive get value");
            String key = request.getKey();
            System.out.println("key==" + key);
            GetValueReply reply;
            if (concurrentHashMap.get(key) != null) {
                System.out.println("key found");
                reply = GetValueReply.newBuilder().setV(concurrentHashMap.get(key)).setStatus(Raft.Status.KeyFound).build();
            } else {
                reply = GetValueReply.newBuilder().setStatus(Raft.Status.KeyNotFound).build();
            }
            System.out.println("node" + state.nodeId + "return" + reply.getStatus());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        // Desc:if(request.getTerm()>)
        // Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
        //
        // Params:
        // args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
        // you call this API
        // reply: the RequestVote Reply Message
        @Override
        public void requestVote(RequestVoteArgs request,
                                StreamObserver<RequestVoteReply> responseObserver) {
            // TODO: Implement this!
            RequestVoteReply requestVoteReply = null;
            boolean success = false;
            if (becomeFollower(request.getTerm(), request.getFrom())) {
                success = true;
            } else {
                if (state.getVotedFor() == Variables.VOTE_FOR_NOONE) {
                    success = true;
                    state.setVotedFor(request.getFrom());
                    state.setRole(Raft.Role.Follower);
                } else {
                    success = false;
                }
            }
            if (success) {
                requestVoteReply = RequestVoteReply.newBuilder().setVoteGranted(success)
                        .setTerm(state.getCurrentTerm().get())
                        .setFrom(state.nodeId)
                        .setTo(request.getFrom())
                        .build();
                state.setVotedFor(request.getFrom());
                taskHolder.addElection();
            } else {
                requestVoteReply = RequestVoteReply.newBuilder().setVoteGranted(false)
                        .setTerm(state.getCurrentTerm().get())
                        .setFrom(state.nodeId)
                        .setTo(request.getFrom())
                        .build();
            }
            responseObserver.onNext(requestVoteReply);
            responseObserver.onCompleted();
        }

        // Desc:
        // Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
        //
        // Params:
        // args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
        // you call this API
        // reply: the AppendEntries Reply Message
        @Override
        public void appendEntries(AppendEntriesArgs request,
                                  StreamObserver<AppendEntriesReply> responseObserver) {
            // TODO: Implement this!
            becomeFollower(request.getTerm(), request.getLeaderId());
            if (request.getEntriesList() == null || request.getEntriesList().size() == 0) {
                AppendEntriesReply appendEntriesReply = AppendEntriesReply.newBuilder()
                        .setSuccess(true)
                        .setTerm(state.currentTerm.get())
                        .setFrom(state.nodeId)
                        .setMatchIndex(state.log.size())
                        .setTo(request.getFrom())
                        .build();
                refresh(request.getLeaderCommit());
                responseObserver.onNext(appendEntriesReply);
                responseObserver.onCompleted();
                return;
            }
            if (request.getLeaderCommit() > state.commitIndex.get()) {
                state.commitIndex.set(Math.min(request.getLeaderCommit(), state.log.size()));
            }
            AppendEntriesReply appendEntriesReply = null;
            boolean success = true;
            System.out.println("got here");
            if (request.getTerm() < state.currentTerm.get()) {
                success = false;
            } else {
                int preIndex = request.getPrevLogIndex();
                if (preIndex == 0) {
                    CopyOnWriteArrayList<LogEntry> list = state.getLog();
                    for (Raft.LogEntry e : request.getEntriesList()) {
                        list.add(e);
                    }
                    success = true;
                } else if (preIndex > state.log.size()) {
                    success = false;
                } else {
                    System.out.println("gogogogogogoogogogogogogogo");
                    LogEntry entry = state.log.get(preIndex - 1);
                    int term = entry.getTerm();
                    if (term != request.getTerm()) {
                        System.out.println("go success false");
                        success = false;
                    } else {
                        CopyOnWriteArrayList<LogEntry> list = state.getLog();
                        System.out.println("go add entries");
                        for (Raft.LogEntry e : request.getEntriesList()) {
                            list.add(e);
                        }
                        success = true;
                    }
                }
            }
//            int size = state.getLog().size();
            refresh(request.getLeaderCommit());
            if (success) {
                appendEntriesReply = AppendEntriesReply.newBuilder()
                        .setSuccess(true)
                        .setTerm(state.currentTerm.get())
                        .setFrom(state.nodeId)
                        .setMatchIndex(state.getLog().size())
                        .setTo(request.getFrom())
                        .build();
            } else {
                appendEntriesReply = AppendEntriesReply.newBuilder()
                        .setSuccess(false)
                        .setTerm(state.currentTerm.get())
                        .setFrom(state.nodeId)
                        .setMatchIndex(state.getLog().size())
                        .setTo(request.getFrom())
                        .build();
            }
            responseObserver.onNext(appendEntriesReply);
            responseObserver.onCompleted();
        }

        public void leaderRefresh() {
            if(state.getCommitIndex().get()==0){
                return;
            }
            LogEntry logEntry = state.getLog().get(state.getCommitIndex().get()-1);
            int op = logEntry.getOpValue();
            System.out.println("op==" + op);
            switch (op) {
                case 0:
                    //put
                    System.out.println("commit o in leader");
                    this.concurrentHashMap.put(logEntry.getKey(), logEntry.getValue());
                    break;
                case 1:
                    System.out.println("commit delete in leader");
                    this.concurrentHashMap.remove(logEntry.getKey());
                    break;
            }
        }

        public void refresh(int leaderCommit) {
            System.out.println("refresh");
            System.out.println("leader" + leaderCommit + "cur" + state.getCommitIndex());
            System.out.println("log size" + state.getLog().size());
            if (state.getLog().size() == 0) {
                return;
            }
            int cur = state.commitIndex.get();
            for (int i = cur; i < leaderCommit; i++) {
                LogEntry logEntry = state.getLog().get(i);
                int op = logEntry.getOpValue();
                System.out.println("op==" + op);
                switch (op) {
                    case 0:
                        //put
                        this.concurrentHashMap.put(logEntry.getKey(), logEntry.getValue());
                        break;
                    case 1:
                        System.out.println("commit delete in slave");
                        this.concurrentHashMap.remove(logEntry.getKey());
                        break;
                }
            }
            state.commitIndex.set(leaderCommit);
        }

        // Desc:
        // Set electionTimeOut as args.Timeout milliseconds.
        // You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
        //
        // Params:
        // args: the heartbeat duration
        // reply: no use
        @Override
        public void setElectionTimeout(SetElectionTimeoutArgs request,
                                       StreamObserver<SetElectionTimeoutReply> responseObserver) {
            int time = request.getTimeout();
            Variables.electionTimeout = time;
            taskHolder.addElection();
            System.out.println("electionTimeOut="+time);
        }

        // Desc:
        // Set heartBeatInterval as args.Interval milliseconds.
        // You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
        //
        // Params:
        // args: the heartbeat duration
        // reply: no use
        @Override
        public void setHeartBeatInterval(SetHeartBeatIntervalArgs request,
                                         StreamObserver<SetHeartBeatIntervalReply> responseObserver) {
            // TODO: Implement this!
            System.out.println("change heartbeart interval");
            int time = request.getInterval();
            Variables.heartBeatInterval = time;
            taskHolder.addHeartBeatWithDelay();
        }

        //NO NEED TO TOUCH THIS FUNCTION
        @Override
        public void checkEvents(CheckEventsArgs request,
                                StreamObserver<CheckEventsReply> responseObserver) {
        }

        public LogEntry appendLocalEntry(ProposeArgs request) {
            LogEntry logEntry = LogEntry.newBuilder().setOp(request.getOp())
                    .setOpValue(request.getOpValue())
                    .setValue(request.getV())
                    .setTerm(state.currentTerm.get())
                    .setUnknownFields(request.getUnknownFields())
                    .setKeyBytes(request.getKeyBytes())
                    .setKey(request.getKey())
                    .build();
            state.getLog().add(logEntry);
            return logEntry;
        }


        public Server getGrpcServer() {
            return this.server;
        }

        public boolean becomeFollower(int term, int leaderId) {
            System.out.println("i am follower now");
            taskHolder.addElection();
            taskHolder.stopHeartBeat();
            if (state.getCurrentTerm().get() < term) {
                state.setLeaderId(leaderId);
                state.getCurrentTerm().set(term);
                state.setRole(Raft.Role.Follower);
                return true;
            }
            return false;
        }
//        public void deleteConflict(int index){
//            for(int i=index;i<state.getLog().size();i++){
//                state.getLog().remove()
//            }
//        }
    }
}
