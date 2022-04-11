package cuhk.asgn;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-10 20:53
 **/
public interface Scheduler {
    ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit);
    ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period,
                                           final TimeUnit unit);
    ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay,
                                              final TimeUnit unit);
    void shutdown();
}
