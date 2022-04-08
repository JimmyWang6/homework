package cuhk.asgn;

import cuhk.asgn.runnable.ElectionTask;
import cuhk.asgn.runnable.HeartBeatTask;

import java.sql.Time;
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
    State state;
    public RaftRunner.RaftNode raftNode;
    TaskHolder(State state, RaftRunner.RaftNode raftNode){
        this.raftNode = raftNode;
        electionTask = new ElectionTask(state,executor,raftNode);
        this.state = state;
        electionFuture = threadPoolExecutor.scheduleAtFixedRate(electionTask,Variables.electionTimeout,Variables.electionTimeout, TimeUnit.MILLISECONDS);
    }
    public void addElection(){
        if(electionFuture!=null){
            System.out.println("cancle"+electionFuture.cancel(true));
        }
        this.electionTask = new ElectionTask(this.state,executor,raftNode);
        electionFuture = threadPoolExecutor.scheduleAtFixedRate(electionTask,Variables.electionTimeout,Variables.electionTimeout, TimeUnit.MILLISECONDS);
    }
    public void addHeartBeat(){
        if(heartFuture!=null){
            heartFuture.cancel(true);
        }
        this.heartBeatTask= new HeartBeatTask(this.state,executor);
        heartFuture = threadPoolExecutor.scheduleAtFixedRate(heartBeatTask,0,50, TimeUnit.MILLISECONDS);
    }
    public void stopHeartBeat(){
        heartFuture.cancel(true);
        heartFuture = null;
    }
    public void stopElection(){
        electionFuture.cancel(true);
        electionFuture = null;
    }

}
