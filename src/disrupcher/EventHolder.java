package disrupcher;

import java.util.concurrent.atomic.AtomicInteger;

public class EventHolder<T> {
    private T event;
    private long sequence;
    private int id;
    private static AtomicInteger idGenerator = new AtomicInteger();

    public void set(T event, long sequence) {
        id = idGenerator.incrementAndGet();
        this.event = event;
        this.sequence = sequence;
    }

    public T getEvent() {
        return event;
    }

    public int getId() {
        return id;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return getId() + "#\t" + getEvent().toString();
    }

    public void clear() {
        id = 0;
        event = null;
    }
}
