package cuhk.asgn.runnable;

import cuhk.asgn.State;
import cuhk.asgn.TaskHolder;
import cuhk.asgn.Variables;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-10 18:06
 **/
public class HeartTask implements Runnable {
    State state;
    int nodeId;
    TaskHolder taskHolder;
    public HeartTask(State state,int nodeId,TaskHolder taskHolder){
        this.state = state;
        this.nodeId = nodeId;
        this.taskHolder = taskHolder;
    }
    @Override
    public void run(){
        System.out.println("node"+nodeId+"heart beat task get ran");
        try {
            int next = state.getNextIndex()[nodeId];
            int pre = state.getMatchIndex()[nodeId];
            int preLogTerm;
            if (pre == 0) {
                preLogTerm = 0;
            } else {
                preLogTerm = state.getLog().get(pre-1).getTerm();
            }
            Raft.AppendEntriesArgs.Builder builder = Raft.AppendEntriesArgs.newBuilder();
            System.out.println("here heartbeat start");
            System.out.println("cur leadrLog size"+state.getLog().size());
            if(state.getLog().size()==0){

            }else{
                System.out.println("next=="+next);
                for(int i=next-1;i<state.getLog().size();i++){
                    builder.addEntries(state.getLog().get(i));
                }
            }
            System.out.println("prepare now");
            try {
                AppendEntryTask appendEntryTask = new AppendEntryTask(state, taskHolder.executor,nodeId,taskHolder);
                taskHolder.executor.submit(appendEntryTask);
//                state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
//                state.getNextIndex()[r.getFrom()] = r.getMatchIndex()+1;
            } catch (Exception e) {
                e.printStackTrace();
            }
//            if (r.getTerm() > state.getCurrentTerm().get()) {
//                //become follower
//                //avoid split brain problem
//                state.getCurrentTerm().set(r.getTerm());
//                state.setVotedFor(Variables.VOTE_FOR_NOONE);
//                state.setRole(Raft.Role.Follower);
//            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }
}
