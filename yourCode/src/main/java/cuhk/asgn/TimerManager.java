package cuhk.asgn;

import java.util.HashMap;
import java.util.concurrent.*;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-10 20:54
 **/
public class TimerManager implements Scheduler {
    private final ScheduledExecutorService executor;
    HashMap<Integer,ScheduledFuture> map;

    public TimerManager(State state) {
        this.executor = new ScheduledThreadPoolExecutor(8,new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return this.executor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return this.executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return this.executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        this.executor.shutdownNow();
    }
}
