package cuhk.asgn;

import raft.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @program: asgn
 * @description: For leader to send appendEntryCommand To Follower
 * @author: Mr.Wang
 * @create: 2022-03-01 00:02
 **/
public class Subject {
    Map<Integer, RaftNodeGrpc.RaftNodeBlockingStub> hostConnectionMap;
    ThreadPoolExecutor executor;
    private volatile static Subject instance;

    public static Subject getInstance(Map map) {
        if (instance == null) {
            synchronized (Subject.class) {
                if (instance == null) {
                    instance = new Subject(map);
                }
            }
        }
        return instance;
    }

    private Subject(Map map) {
        executor = new ThreadPoolExecutor(8, 30,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue(30), new ThreadPoolExecutor.AbortPolicy());
        this.hostConnectionMap = map;
    }


    public boolean appendEntry(Raft.LogEntry logEntry, State state, Object o) throws InterruptedException {
        final List<Raft.AppendEntriesReply> replyList = new ArrayList<>();
        for (Map.Entry entry : hostConnectionMap.entrySet()) {
            int id = (int)entry.getKey();
            if(id==state.leaderId){
                //don't send to leader(myself)
                continue;
            }
            final Raft.AppendEntriesArgs appendEntriesArgs = Raft
                    .AppendEntriesArgs
                    .newBuilder()
                    .addEntries(logEntry)
                    .setLeaderId(state.leaderId)
                    .setFrom(state.leaderId)
                    .setTo(id)
                    .setTerm(state.currentTerm.get())
                    .setLeaderCommit(state.commitIndex.get())
                    .build();
            final RaftNodeGrpc.RaftNodeBlockingStub stub = (RaftNodeGrpc.RaftNodeBlockingStub) entry.getValue();
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    Raft.AppendEntriesReply entriesReply = null;
                    try {
                        entriesReply = stub.appendEntries(appendEntriesArgs);
                    } catch (Exception e) {
                        //there is a timeOut;
                        //addToRetryList;
                    }
                    //in this situation, the Thread would get in this position only if
                    //there is a timeout or get the correct Reply;
                    if (entriesReply != null) {
                        replyList.add(entriesReply);
                    }
                }
            };
            executor.submit(runnable);
        }
        int counter = 0;
        for (Raft.AppendEntriesReply reply : replyList) {
            if (reply.getSuccess()) {
                counter++;
            }
        }
        if (counter > hostConnectionMap.size() / 2) {
            return true;
        } else {
            return false;
        }
    }
}
