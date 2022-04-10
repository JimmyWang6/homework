package cuhk.asgn.runnable;

import cuhk.asgn.State;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * @program: asgn
 * @description:
 * @author: Mr.Wang
 * @create: 2022-04-10 02:50
 **/
public class Waiter {
    State state;
    public Waiter(int curIndex, State state){
        this.state = state;
        this.index = curIndex;
    }
    public int index;
    public boolean couldReturn(){
        int counter = 0;
        int size = state.hostConnectionMap.size();
        for(int i=0;i<size;i++){
            if(state.getMatchIndex()[i]>=index){
                counter++;
            }
        }
        if(counter>size/2){
            return true;
        }
        return false;
//        if((alreadyReturn.size()+1)>(total/2)){
//            System.out.println("majority already commit"+index);
//            for(Integer i:alreadyReturn){
//                System.out.print(i);
//            }
//            System.out.println();
//            return true;
//        }
//        return false;
    }
    public void setIndex(int index){
        this.index = index;
    }
}
