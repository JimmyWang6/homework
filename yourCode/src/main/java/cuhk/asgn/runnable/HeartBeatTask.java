package cuhk.asgn.runnable;

import cuhk.asgn.RaftRunner;
import cuhk.asgn.State;
import cuhk.asgn.Variables;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
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

    public HeartBeatTask(State state, ThreadPoolExecutor threadPool) {
        this.state = state;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        if (!this.state.getRole().equals(Raft.Role.Leader)) {
            return;
        }
        for (Map.Entry<Integer, RaftNodeGrpc.RaftNodeBlockingStub> entry : state.getHostConnectionMap().entrySet()) {
            final int nodeId = entry.getKey();
            final RaftNodeGrpc.RaftNodeBlockingStub stub = entry.getValue();
            if (nodeId == state.getNodeId()) {
                //not send to myself;
                continue;
            }
            threadPool.execute(new Runnable() {
                final Raft.AppendEntriesArgs appendEntriesArgs = Raft.AppendEntriesArgs.newBuilder()
                        .setPrevLogIndex(state.getCommitIndex().get())
                        .setLeaderId(state.getNodeId())
                        .setFrom(state.getNodeId())
                        .setTo(nodeId)
                        .setTerm(state.getCurrentTerm().get())
                        .build();
                @Override
                public void run() {
                    try{
                        Raft.AppendEntriesReply r = stub.appendEntries(appendEntriesArgs);
                        if(r.getTerm() > state.getCurrentTerm().get()){
                            //become follower
                            //avoid split brain problem
                            state.getCurrentTerm().set(r.getTerm());
                            state.setVotedFor(Variables.VOTE_FOR_NOONE);
                            state.setRole(Raft.Role.Follower);
                        }
                    }catch (Exception e){
                        System.out.println("client"+nodeId+"heartBeat no reply");
                    }
                }
            });
        }

    }
}
