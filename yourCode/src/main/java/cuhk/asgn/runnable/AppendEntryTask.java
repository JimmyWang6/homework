package cuhk.asgn.runnable;

import cuhk.asgn.State;
import raft.Raft;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-09 03:18
 **/
public class AppendEntryTask implements Callable {
    State state;
    ThreadPoolExecutor threadPool;
    int nodeId;
    public AppendEntryTask(State state, ThreadPoolExecutor threadPool,int nodeId) {
        this.state = state;
        this.threadPool = threadPool;
        this.nodeId = nodeId;
    }
    @Override
    public Object call() throws Exception {
        int next = state.getNextIndex()[nodeId];
        int preLogTerm;
        if(next==1){
            preLogTerm = 0;
        }else{
            preLogTerm = state.getLog().get(next-2).getTerm();
        }
        Raft.AppendEntriesArgs appendEntriesArgs = Raft.AppendEntriesArgs.newBuilder()
                    .setTo(nodeId)
                    .setFrom(state.getNodeId())
                    .setTerm(state.getCurrentTerm().get())
                    .setLeaderId(state.getNodeId())
                    .setPrevLogTerm(preLogTerm)
                    .setLeaderCommit(state.getCommitIndex().get())
                    .setPrevLogIndex(next-1)
                    .build();
        Raft.AppendEntriesReply appendEntriesReply = state.hostConnectionMap.get(nodeId).appendEntries(appendEntriesArgs);
        return appendEntriesReply;
    }
}
