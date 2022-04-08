package cuhk.asgn.runnable;

import cuhk.asgn.State;
import raft.Raft;
import raft.RaftNodeGrpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

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
            idMap.put(nodeId,appendEntryTask);
        }
        int size = list.size();
        for(int i=0;i<size;i++){
            Future<Raft.AppendEntriesReply> future = list.get(i);
            try{
                Raft.AppendEntriesReply r =future.get(100, TimeUnit.MILLISECONDS);
                //return successfully
                idMap.remove(i);
            }catch (TimeoutException e){

            }
        }
        return idMap;
    }
}
