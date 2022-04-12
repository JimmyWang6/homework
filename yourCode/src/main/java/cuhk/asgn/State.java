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
    int lastLogTerm;
    int votedFor;
    int nodeId;
    AtomicInteger commitIndex;
    int leaderId ;
    int [] nextIndex;
    int [] matchIndex;
    //Thread safe List
    public volatile CopyOnWriteArrayList<Raft.LogEntry> log;
    public State(){
        hostConnectionMap = new HashMap<>();
        //currentTerm init to zero
        currentTerm = new AtomicInteger(0);
        commitIndex = new AtomicInteger(0);
        //initial role is follower
        role = Raft.Role.Follower;
        //current vote for no one
        votedFor = Variables.VOTE_FOR_NOONE;
        leaderId = Variables.NO_LEADER;
        //initial log
        log = new CopyOnWriteArrayList<>();
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

    public Raft.Role getRole() {
        return role;
    }

    public void setRole(Raft.Role role) {
        this.role = role;
    }

    public Timer getTimer() {
        return timer;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }

    public Map<Integer, RaftNodeGrpc.RaftNodeBlockingStub> getHostConnectionMap() {
        return hostConnectionMap;
    }

    public void setHostConnectionMap(Map<Integer, RaftNodeGrpc.RaftNodeBlockingStub> hostConnectionMap) {
        this.hostConnectionMap = hostConnectionMap;
    }

    public AtomicInteger getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(AtomicInteger currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public AtomicInteger getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(AtomicInteger commitIndex) {
        this.commitIndex = commitIndex;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public CopyOnWriteArrayList<Raft.LogEntry> getLog() {
        return log;
    }

    public void setLog(CopyOnWriteArrayList<Raft.LogEntry> log) {
        this.log = log;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public int[] getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int[] nextIndex) {
        this.nextIndex = nextIndex;
    }

    public int[] getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int[] matchIndex) {
        this.matchIndex = matchIndex;
    }
}
