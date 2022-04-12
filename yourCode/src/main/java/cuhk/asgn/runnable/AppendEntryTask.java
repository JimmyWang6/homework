package cuhk.asgn.runnable;

import cuhk.asgn.State;
import cuhk.asgn.TaskHolder;
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
public class AppendEntryTask implements Runnable {
    State state;
    ThreadPoolExecutor threadPool;
    TaskHolder taskHolder;
    int nodeId;
    public AppendEntryTask(State state, ThreadPoolExecutor threadPool, int nodeId, TaskHolder taskHolder) {
        this.state = state;
        this.threadPool = threadPool;
        this.nodeId = nodeId;
        this.taskHolder = taskHolder;
    }
    @Override
    public void run(){
        try{
            System.out.println("call here");
            int next = state.getNextIndex()[nodeId];
            int pre = state.getMatchIndex()[nodeId];
            int preLogTerm;
            if(pre==0){
                //not send any log
                preLogTerm = 0;
            }else{
                preLogTerm = state.getLog().get(pre-1).getTerm();
            }
            Raft.AppendEntriesArgs.Builder builder = Raft.AppendEntriesArgs.newBuilder();
            System.out.println(nodeId+"cur next=="+next);
            if(state.getLog().size()==0){

            }else{
                System.out.println("next=="+next);
                for(int i=next-1;i<state.log.size();i++){
                    builder.addEntries(state.log.get(i));
                }
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
            Raft.AppendEntriesReply r= state.hostConnectionMap.get(nodeId).appendEntries(appendEntriesArgs);
            state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
            state.getNextIndex()[r.getFrom()] = r.getMatchIndex()+1;
            taskHolder.returnCheck();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
