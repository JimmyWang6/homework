package cuhk.asgn;

import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.Map;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-03-03 02:08
 **/
public class NotifyTask implements Callback{
    @Override
    public void slove() {

    }
//    State state;
//    Raft.RequestVoteReply requestVoteReply;
//    NotifyTask(State state){
//        this.state = state;
//    }
//    @Override
//    public void run() {
//        this.slove();
//    }
//
//    @Override
//    public void slove(){
//        for(Map.Entry<Integer, RaftNodeGrpc.RaftNodeBlockingStub> entry:state.hostConnectionMap.entrySet()){
//            RaftNodeGrpc.RaftNodeBlockingStub stub = entry.getValue();
//            Raft.RequestVoteArgs requestVoteArg = Raft.RequestVoteArgs
//                    .newBuilder()
//                    .setFrom(state.nodeId)
//                    .setTo(entry.getKey())
//                    .setTerm(state.currentTerm.get())
//                    .setCandidateId(state.nodeId)
//                    .setLastLogIndex(state.log.size()-1)
////                    .setLastLogTerm(state.log.get(state.))
//                    .build();
//            this.requestVoteReply = stub.requestVote(requestVoteArg);
//        }
//    }
}
