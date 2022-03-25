package cuhk.asgn;

import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-03-03 01:59
 **/
public class Timer implements Runnable {
    ScheduledExecutorService scheduledExecutorService;
    ScheduledFuture sc;
    Raft.RequestVoteReply requestVoteReply;
    State state;
    Timer(State state){
        this.state = state;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }
    public void timeOut(int time){
       //stop old Timer
        if(sc==null){
            this.sc = scheduledExecutorService.scheduleAtFixedRate(
                    new NotifyTask(this.state),
                    0,
                    time,
                    TimeUnit.MILLISECONDS);
        }else{
            if(sc.cancel(true)==true){
                //start new Timer
                this.sc = scheduledExecutorService.scheduleAtFixedRate(
                        new NotifyTask(this.state),
                        0,
                        time,
                        TimeUnit.MILLISECONDS);
            }else{
                System.out.println("timer error");
            }
        }
    }

    @Override
    public void run() {

    }

}
