package timedelayqueue;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import java.util.*;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TimeDelayQueue {

    // a comparator to sort messages
    private class PubSubMessageComparator implements Comparator<PubSubMessage> {
        public int compare(PubSubMessage msg1, PubSubMessage msg2) {
            return msg1.getTimestamp().compareTo(msg2.getTimestamp());
        }
    }

    private PriorityBlockingQueue<PubSubMessage> TDQ = new PriorityBlockingQueue<PubSubMessage>(11, new PubSubMessageComparator());

    private final int queueDelay;

    private int lifetimeMsgCount;
    //private int totalTime = 0;
    private int totalOperations = 0;

    /**
     * Create a new TimeDelayQueue
     *
     * @param delay  the delay, in milliseconds, that the queue can tolerate, >= 0
     **/


    public TimeDelayQueue(int delay) {
        this.queueDelay = delay;
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public boolean add(PubSubMessage msg) {

        boolean addSuccess = TDQ.add(msg);

        if(addSuccess) {
            lifetimeMsgCount++;
            //helpful for task 2
            //totalTime += msg.getTimestamp().getTime();
            totalOperations++;
        }

        return addSuccess;
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     * @return
     */
    public long getTotalMsgCount() {
        return lifetimeMsgCount;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
     synchronized public PubSubMessage getNext() {

        //Checking and removing expired transient messages
        PubSubMessage[] array = new PubSubMessage[TDQ.size()];
        Object[] tempArray = TDQ.toArray(array);

        for(PubSubMessage msg1 : array){
            if( msg1.isTransient()){
                if(((TransientPubSubMessage) msg1).getLifetime() + msg1.getTimestamp().getTime() < System.currentTimeMillis()){
                    TDQ.remove(msg1);
                }
            }
        }

        //Normal getNext stuff
        if(TDQ.isEmpty()) {
            return PubSubMessage.NO_MSG;

        } else if ((System.currentTimeMillis() - TDQ.peek().getTimestamp().getTime()) >= queueDelay){

            totalOperations++;
            return TDQ.poll();

        }

        return PubSubMessage.NO_MSG;

    }

    /*private int getTotalTime (){
        return totalTime;
    }*/

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {

        List<PubSubMessage> listOfTDQ;
        List<Long> applicableTDQ;

        int max = 0;

        listOfTDQ = TDQ.stream().sorted(Comparator.comparing(PubSubMessage::getTimestamp)).collect(Collectors.toList());



        //Collections.sort(arrayOfTDQ);

        for(int i = 0; i < listOfTDQ.size(); i++) {
            long comprableTDQ = listOfTDQ.get(i).getTimestamp().getTime();
            applicableTDQ = listOfTDQ.stream().map(j -> j.getTimestamp().getTime()).filter(j -> Math.abs(j-comprableTDQ) <= timeWindow).toList();
            if(max < applicableTDQ.size()){
                max = applicableTDQ.size();
            }
        }
        return max;
    }



}
