package cuhk.asgn.runnable;

import cuhk.asgn.RaftRunner;
import cuhk.asgn.State;
import cuhk.asgn.TaskHolder;
import cuhk.asgn.Variables;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-07 17:01
 **/
public class HeartBeatTask implements Runnable {
    ThreadPoolExecutor threadPool;
    State state;
    HashMap<Integer, Raft.AppendEntriesArgs> hashMap;
    TaskHolder taskHolder;

    public HeartBeatTask(State state, ThreadPoolExecutor threadPool, HashMap hashMap, TaskHolder taskHolder) {
        this.state = state;
        this.taskHolder = taskHolder;
        this.threadPool = threadPool;
        this.hashMap = hashMap;
    }

    public void setHashMap(HashMap hashMap) {
        this.hashMap = hashMap;
    }

    @Override
    public void run() {
        if (!this.state.getRole().equals(Raft.Role.Leader)) {
            return;
        }
        System.out.println("heart beart start,lost msg size = "+hashMap.size());
        ArrayList<Future> list = new ArrayList<>();
        for (Map.Entry<Integer, RaftNodeGrpc.RaftNodeBlockingStub> entry : state.getHostConnectionMap().entrySet()) {
            final int nodeId = entry.getKey();
            final RaftNodeGrpc.RaftNodeBlockingStub stub = entry.getValue();
            if (nodeId == state.getNodeId()) {
                //not send to myself;
                continue;
            }
            final Raft.AppendEntriesArgs appendEntriesArgs;
            int preLogTerm = 0;
            int next = state.getNextIndex()[nodeId];
            int pre = state.getMatchIndex()[nodeId];
            if (pre!= 0) {
                preLogTerm = state.getLog().get(pre-1).getTerm();
            }
//            if (hashMap.get(nodeId) == null) {
//                appendEntriesArgs = Raft.AppendEntriesArgs.newBuilder()
//                        .setPrevLogIndex(pre)
//                        .setLeaderId(state.getNodeId())
//                        .setFrom(state.getNodeId())
//                        .setPrevLogTerm(preLogTerm)
//                        .setLeaderCommit(state.getCommitIndex().get())
//                        .setTo(nodeId)
//                        .setTerm(state.getCurrentTerm().get())
//                        .build();
//            } else {
                Raft.AppendEntriesArgs.Builder builder = Raft.AppendEntriesArgs.newBuilder();
                for(int i=next-1;i<state.getLog().size();i++){
                    builder.addEntries(state.getLog().get(i));
                }
                appendEntriesArgs = builder
                        .setTo(nodeId)
                        .setFrom(state.getNodeId())
                        .setTerm(state.getCurrentTerm().get())
                        .setLeaderId(state.getNodeId())
                        .setPrevLogTerm(preLogTerm)
                        .setLeaderCommit(state.getCommitIndex().get())
                        .setPrevLogIndex(pre)
                        .build();
//            }
            list.add(threadPool.submit(new Callable() {
                @Override
                public Object call() {
                    Raft.AppendEntriesReply r = stub.appendEntries(appendEntriesArgs);
                    return r;
                }
            }));
        }
        final AtomicInteger atomicInteger = new AtomicInteger(1);
        for (int i = 0; i < list.size(); i++) {
            final Future<Raft.AppendEntriesReply> future = list.get(i);
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Raft.AppendEntriesReply r = future.get(100, TimeUnit.MILLISECONDS);
                        if (r.getTerm() > state.getCurrentTerm().get()) {
                            //become follower
                            //avoid split brain problem
                            state.getCurrentTerm().set(r.getTerm());
                            state.setVotedFor(Variables.VOTE_FOR_NOONE);
                            state.setRole(Raft.Role.Follower);
                            state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
                        }
                        hashMap.remove(r.getFrom());
                    } catch (Exception e) {
                        //timeout
                    }finally {
                        atomicInteger.getAndIncrement();
                    }
                }
            });
        }
        while(atomicInteger.get()<state.getHostConnectionMap().size()){

        }
        taskHolder.returnCheck();

    }
}