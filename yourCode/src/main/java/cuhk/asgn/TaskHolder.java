package cuhk.asgn;

import cuhk.asgn.runnable.ElectionTask;
import cuhk.asgn.runnable.HeartBeatTask;
import cuhk.asgn.runnable.LeaderTask;
import cuhk.asgn.runnable.Waiter;
import raft.Raft;

import java.sql.Time;
import java.util.*;
import java.util.concurrent.*;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-07 18:48
 **/
public class TaskHolder {
    ScheduledThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(5);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 30,1,TimeUnit.MINUTES,
                new ArrayBlockingQueue(30), new ThreadPoolExecutor.AbortPolicy());
    public ElectionTask electionTask;
    public HeartBeatTask heartBeatTask;
    ScheduledFuture<?> electionFuture;
    ScheduledFuture<?> heartFuture;
    HashMap<Integer,HeartBeatTask> remainTask;
    List<Waiter> waiterList = new ArrayList<>();
    State state;
    public RaftRunner.RaftNode raftNode;
    TaskHolder(State state, RaftRunner.RaftNode raftNode){
        remainTask = new HashMap<>();
        this.raftNode = raftNode;
        electionTask = new ElectionTask(state,executor,raftNode);
        this.state = state;
        electionFuture = threadPoolExecutor.scheduleAtFixedRate(electionTask,Variables.electionTimeout,Variables.electionTimeout, TimeUnit.MILLISECONDS);
    }
    public void addElection(){
        if(electionFuture!=null){
            electionFuture.cancel(true);
        }
        this.electionTask = new ElectionTask(this.state,executor,raftNode);
        electionFuture = threadPoolExecutor.scheduleAtFixedRate(electionTask,Variables.electionTimeout,Variables.electionTimeout, TimeUnit.MILLISECONDS);
    }
    public Waiter leaderTask() {
        Waiter waiter = new Waiter(state.getCommitIndex().get(),state);
        Callable callable = new LeaderTask(state,executor);
        Future<Set> future = executor.submit(callable);
        Set<Integer> set = new HashSet<>();
        System.out.println("do leader work");
        try{
            set = future.get(100,TimeUnit.MILLISECONDS);
//            success = state.getHostConnectionMap().size() - remainTask.size()-1;
//            this.addHeartBeatWithDelay();
        }catch (Exception e){
            System.out.println("there is an append entry fail");
            e.printStackTrace();
        }
        this.returnCheck();
        return waiter;
    }
    public void addHeartBeat(){
        if(heartFuture!=null){
            heartFuture.cancel(true);
        }
        this.heartBeatTask= new HeartBeatTask(this.state,executor,this.remainTask,this);
        heartFuture = threadPoolExecutor.scheduleAtFixedRate(heartBeatTask,0,Variables.heartBeatInterval, TimeUnit.MILLISECONDS);
    }
    public void addHeartBeatWithDelay(){
        if(heartFuture!=null){
            heartFuture.cancel(true);
        }
        this.heartBeatTask= new HeartBeatTask(this.state,executor,this.remainTask,this);
        heartFuture = threadPoolExecutor.scheduleAtFixedRate(heartBeatTask,Variables.heartBeatInterval,Variables.heartBeatInterval, TimeUnit.MILLISECONDS);
    }
    public void stopHeartBeat(){
        if(heartFuture!=null){
            heartFuture.cancel(true);
        }
        heartFuture = null;
    }
    public void stopElection(){
        electionFuture.cancel(true);
        electionFuture = null;
    }

    public boolean waitUntilMajority(Raft.LogEntry logEntry, Raft.ProposeArgs request) throws InterruptedException {
        Waiter waiter = this.leaderTask();
        if(waiter.couldReturn()){

            return true;
        }
        waiterList.add(waiter);
        System.out.println("waiter size");
        System.out.println(waiterList.size());
        return false;
//        synchronized (waiter){
//            //wait to notify
//            System.out.println("wait here until next reply");
//            waiter.wait();
//        }
    }
    public boolean waiterCheck(Waiter waiter){
        if(waiter.couldReturn()){
            if(state.log.size()==0){

            }else{
                state.commitIndex.getAndIncrement();
            }
            return true;
        }
        return false;
    }
    public void returnCheck(){
        Iterator<Waiter> iterator = waiterList.iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next();
            if(waiterCheck(waiter)){
                iterator.remove();
            }

        }
    }

}
