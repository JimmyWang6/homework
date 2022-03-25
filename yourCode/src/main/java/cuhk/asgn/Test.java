package cuhk.asgn;

import raft.Raft;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-03-01 01:09
 **/
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        args = new String[]{"8888", "8888,9999", "1", "30", "30"};
        String ports = args[1];
        int myport = Integer.parseInt(args[0]);
        int nodeID = Integer.parseInt(args[2]);
        int heartBeatInterval = Integer.parseInt(args[3]);
        int electionTimeout = Integer.parseInt(args[4]);
        String[] portStrings = ports.split(",");

        // A map where
        // 		the key is the node id
        //		the value is the {hostname:port}
        Map<Integer, Integer> hostmap = new HashMap<>();
        for (int x = 0; x < portStrings.length; x++) {
            hostmap.put(x, Integer.valueOf(portStrings[x]));
        }
        RaftRunner.RaftNode node = RaftRunner.NewRaftNode(8888, hostmap, 0, heartBeatInterval, electionTimeout);
        RaftRunner.RaftNode node1 = RaftRunner.NewRaftNode(9999, hostmap, 1, heartBeatInterval, electionTimeout);
        Raft.ProposeArgs proposeArgs = Raft.ProposeArgs.newBuilder()
                .setOp(Raft.Operation.Put)
                .setKey("1")
                .setV(0)
                .build();
        Thread.sleep(2000);
        node.state.hostConnectionMap.get(9999).propose(proposeArgs);
        System.out.println(node1.state);


    }
}
