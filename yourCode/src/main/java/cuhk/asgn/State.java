package cuhk.asgn;

import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-02-28 23:09
 **/

public class State {
    Raft.Role role;
    Timer timer;
    public Map<Integer, RaftNodeGrpc.RaftNodeBlockingStub> hostConnectionMap;
    AtomicInteger currentTerm;
    int votedFor;
    int nodeId;
    AtomicInteger commitIndex;
    int leaderId ;
    CopyOnWriteArrayList<Integer> nextIndex;
    CopyOnWriteArrayList<Integer> matchIndex;
    //Thread safe List
    CopyOnWriteArrayList<Raft.LogEntry> log;
    public State(){
        hostConnectionMap = new HashMap<>();
        //currentTerm init to zero
        currentTerm = new AtomicInteger(0);
        commitIndex = new AtomicInteger(0);
        //initial role is follower
        role = Raft.Role.Leader;
        //current vote for no one
        votedFor = Variables.VOTE_FOR_NOONE;
        leaderId = Variables.NO_LEADER;
        //initial log
        log = new CopyOnWriteArrayList<>();
        nextIndex = new CopyOnWriteArrayList<>();
        matchIndex = new CopyOnWriteArrayList<>();
        this.timer = new Timer(this);
    }

    @Override
    public String toString() {
        return "State{" +
                "role=" + role +
                ", hostConnectionMap=" + hostConnectionMap +
                ", currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", nodeId=" + nodeId +
                ", commitIndex=" + commitIndex +
                ", leaderId=" + leaderId +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                ", log=" + log +
                '}';
    }
}
