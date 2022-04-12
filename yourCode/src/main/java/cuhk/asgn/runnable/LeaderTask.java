package cuhk.asgn.runnable;

import cuhk.asgn.RaftRunner;
import cuhk.asgn.State;
import cuhk.asgn.TaskHolder;
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
public class LeaderTask implements Runnable {
    ThreadPoolExecutor threadPool;
    State state;
    TaskHolder taskHolder;
    HashMap<Integer,AppendEntryTask> idMap;
    public LeaderTask(State state, ThreadPoolExecutor threadPool,TaskHolder taskHolder) {
        this.state = state;
        this.threadPool = threadPool;
        idMap = new HashMap<>();
        this.taskHolder = taskHolder;
    }

    @Override
    public void run(){
        try{
            System.out.println("leader come into power,send append entries rpc");
            ArrayList<Future> list = new ArrayList<>();
            for (Map.Entry<Integer, RaftNodeGrpc.RaftNodeBlockingStub> entry : state.getHostConnectionMap().entrySet()) {
                final int nodeId = entry.getKey();
                final RaftNodeGrpc.RaftNodeBlockingStub stub = entry.getValue();
                if (nodeId == state.getNodeId()) {
                    //not send to myself;
                    continue;
                }
                AppendEntryTask appendEntryTask = new AppendEntryTask(state, threadPool,nodeId,taskHolder);
//                Future<Raft.AppendEntriesReply> future = threadPool.submit(appendEntryTask);
//                list.add(future);
            }
            int size = list.size();
            final Set<Integer> successReturn = new HashSet<>();
            final AtomicInteger atomicInteger = new AtomicInteger(1);
            final TaskHolder holder = this.taskHolder;
            for(int i=0;i<size;i++){
                final Future<Raft.AppendEntriesReply> future = list.get(i);
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try{
                            Raft.AppendEntriesReply r =future.get(100, TimeUnit.MILLISECONDS);
                            //return successfully
                            System.out.println("could get reply");
//                            taskHolder.resetBeat(r.getFrom());
                            state.getMatchIndex()[r.getFrom()] = r.getMatchIndex();
                            state.getNextIndex()[r.getFrom()] = r.getMatchIndex()+1;
//                continueTask(r);
                            successReturn.add(r.getFrom());
                            taskHolder.returnCheck();
//                            holder.resetBeat(r.getFrom());
                        }catch (Exception e){
                            e.printStackTrace();
                        }finally {
                            atomicInteger.getAndIncrement();
                        }
                    }
                });
//                taskHolder.addHeartBeat();
            }
//            while(atomicInteger.get()<state.getHostConnectionMap().size()){
//
//            }
//            System.out.println("successReturn size");
//            System.out.println(successReturn.size());
//            taskHolder.returnCheck();
        }catch (Exception e){
            e.printStackTrace();
        }

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
