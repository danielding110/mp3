package timedelayqueue;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

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

        if(TDQ.isEmpty()) {
            return PubSubMessage.NO_MSG;

        } else if ((System.currentTimeMillis() - TDQ.peek().getTimestamp().getTime()) >= queueDelay){

            return TDQ.poll();

        }

        return PubSubMessage.NO_MSG;

    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {
        return -1;
    }

}
