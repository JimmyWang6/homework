package cuhk.asgn.runnable;

import cuhk.asgn.RaftRunner;
import cuhk.asgn.State;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-09 03:57
 **/
public class LeaderTask implements Callable {
    ThreadPoolExecutor threadPool;
    State state;
    HashMap<Integer,AppendEntryTask> idMap;
    public LeaderTask(State state, ThreadPoolExecutor threadPool) {
        this.state = state;
        this.threadPool = threadPool;
        idMap = new HashMap<>();
    }

    @Override
    public Object call() throws Exception {
        System.out.println("leader come into power,send append entries rpc");
        ArrayList<Future> list = new ArrayList<>();
        for (Map.Entry<Integer, RaftNodeGrpc.RaftNodeBlockingStub> entry : state.getHostConnectionMap().entrySet()) {
            final int nodeId = entry.getKey();
            final RaftNodeGrpc.RaftNodeBlockingStub stub = entry.getValue();
            if (nodeId == state.getNodeId()) {
                //not send to myself;
                continue;
            }
            AppendEntryTask appendEntryTask = new AppendEntryTask(state, threadPool,nodeId);
            Future<Raft.AppendEntriesReply> future = threadPool.submit(appendEntryTask);
            list.add(future);
        }
        int size = list.size();
        final Set<Integer> successReturn = new HashSet<>();
        final AtomicInteger atomicInteger = new AtomicInteger(1);
        for(int i=0;i<size;i++){
            final Future<Raft.AppendEntriesReply> future = list.get(i);
            System.out.println("no bug");
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try{
                        System.out.println("get bug");
                        Raft.AppendEntriesReply r =future.get(90, TimeUnit.MILLISECONDS);
                        //return successfully
                        System.out.println("could get reply");
                        state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
                        state.getNextIndex()[r.getFrom()] = r.getMatchIndex()+1;
//                continueTask(r);
                        successReturn.add(r.getFrom());
                    }catch (Exception e){
                        e.printStackTrace();
                    }finally {
                        atomicInteger.getAndIncrement();
                    }
                }
            });
        }
        while(atomicInteger.get()<state.getHostConnectionMap().size()){

        }
        System.out.println("successReturn size");
        System.out.println(successReturn.size());
        return successReturn;
    }
//    public void continueTask(Raft.AppendEntriesReply r){
//        if(r.getSuccess()){
//            //update nextIndex and matchIndex
//            state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
//        }else{
//            //decrease nextIndex and retry
//            state.getNextIndex()[r.getFrom()]--;
//            AppendEntryTask appendEntryTask = new AppendEntryTask(state, threadPool,r.getFrom());
//
//        }
//    }
}
