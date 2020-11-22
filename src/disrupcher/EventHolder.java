package disrupcher;

public class EventHolder<T> {
    private T event;
    private long sequence;

    public void set(T event, long sequence) {
        this.event = event;
        this.sequence = sequence;
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
}
