package cuhk.asgn.runnable;

import cuhk.asgn.State;
import cuhk.asgn.TaskHolder;
import cuhk.asgn.Variables;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.concurrent.Callable;

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
                //not send any log
                preLogTerm = 0;
            } else {
                preLogTerm = state.getLog().get(pre-1).getTerm();
            }
            Raft.AppendEntriesArgs.Builder builder = Raft.AppendEntriesArgs.newBuilder();
            System.out.println("here heartbeat start");
            if (state.getLog().size() == 0) {

            } else {
                for (int i = next - 1; i < state.getLog().size(); i++) {
                    builder.addEntries(state.getLog().get(i));
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
            Raft.AppendEntriesReply r = null;
            System.out.println("prepare now");
            try {
                r = state.hostConnectionMap.get(nodeId).appendEntries(appendEntriesArgs);
                System.out.println("here get reply from" + r.getFrom());
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(r==null){
                System.out.println("can't get heartbeat reply");
                return;
            }
            if (r.getTerm() > state.getCurrentTerm().get()) {
                //become follower
                //avoid split brain problem
                state.getCurrentTerm().set(r.getTerm());
                state.setVotedFor(Variables.VOTE_FOR_NOONE);
                state.setRole(Raft.Role.Follower);
            }
            state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
            state.getNextIndex()[r.getFrom()] = r.getMatchIndex() + 1;
            taskHolder.resetBeat(r.getFrom());
            taskHolder.returnCheck();
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
