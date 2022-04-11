package cuhk.asgn.runnable;

import cuhk.asgn.State;
import raft.Raft;

import java.util.ArrayList;
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
        try{
            System.out.println("call here");
            int next = state.getNextIndex()[nodeId];
            int pre = state.getMatchIndex()[nodeId];
            int preLogTerm;
            if(pre==0){
                //not send any log
                preLogTerm = 0;
            }else{
                preLogTerm = state.getLog().get(pre).getTerm();
            }
            Raft.AppendEntriesArgs.Builder builder = Raft.AppendEntriesArgs.newBuilder();
            System.out.println(nodeId+"cur next=="+next);
            for(int i=next-1;i<state.getLog().size();i++){
                builder.addEntries(state.getLog().get(i));
            }
            Raft.AppendEntriesArgs appendEntriesArgs = builder
                    .setTo(nodeId)
                    .setFrom(state.getNodeId())
                    .setTerm(state.getCurrentTerm().get())
                    .setLeaderId(state.getNodeId())
                    .setPrevLogTerm(preLogTerm)
                    .setLeaderCommit(state.getCommitIndex().get())
                    .setPrevLogIndex(pre)
                    .build();
            Raft.AppendEntriesReply appendEntriesReply = state.hostConnectionMap.get(nodeId).appendEntries(appendEntriesArgs);
            System.out.println("get reply");
            return appendEntriesReply;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
