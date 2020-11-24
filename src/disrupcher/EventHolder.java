package disrupcher;

import utils.NanoClock;

public class EventHolder<T> {
    private T event;
    private long sequence;
    private long nanoTime;

    public void set(T event, long sequence) {
        this.event = event;
        this.sequence = sequence;
        this.nanoTime = NanoClock.getNanoTimeNow();
    }

    public T getEvent() {
        return event;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return getSequence() + "#\t" + getEvent();
    }

    public void clear() {
        event = null;
        sequence = 0;
    }

    public long getAge() {
        return NanoClock.getNanoTimeNow() - this.nanoTime;
    }
}
