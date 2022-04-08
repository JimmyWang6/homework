package cuhk.asgn;

import raft.Raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-03-01 01:09
 **/
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        args = new String[]{"8888", "2222,3333,4444,5555,6666,7777", "1", "10000", "10000"};
        String ports = args[1];
        int myport = Integer.parseInt(args[0]);
        int nodeID = Integer.parseInt(args[2]);
        int heartBeatInterval = Integer.parseInt(args[3]);
        int electionTimeout = Integer.parseInt(args[4]);
        Variables.heartBeatInterval = heartBeatInterval;
        Variables.electionTimeout = electionTimeout;
        String[] portStrings = ports.split(",");

        // A map where
        // 		the key is the node id
        //		the value is the {hostname:port}
        Map<Integer, Integer> hostmap = new HashMap<>();
        for (int x = 0; x < portStrings.length; x++) {
            hostmap.put(x, Integer.valueOf(portStrings[x]));
        }
        RaftRunner.RaftNode node0 = RaftRunner.NewRaftNode(2222, hostmap, 0, heartBeatInterval, 1000);
        RaftRunner.RaftNode node1 = RaftRunner.NewRaftNode(3333, hostmap, 1, heartBeatInterval, 2000);
        RaftRunner.RaftNode node2 = RaftRunner.NewRaftNode(4444, hostmap, 2, heartBeatInterval, 3000);
        RaftRunner.RaftNode node3 = RaftRunner.NewRaftNode(5555, hostmap, 3, heartBeatInterval, 1000);
        RaftRunner.RaftNode node4 = RaftRunner.NewRaftNode(6666, hostmap, 4, heartBeatInterval, 2000);
        RaftRunner.RaftNode node5 = RaftRunner.NewRaftNode(7777, hostmap, 5, heartBeatInterval, 3000);
        List<RaftRunner.RaftNode> list = new ArrayList<>();
        list.add(node1);
        list.add(node0);
        list.add(node3);
        list.add(node4);
        list.add(node5);
        list.add(node2);
    }
}
