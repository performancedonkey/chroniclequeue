package simulation;

public interface Batcher<T> {

    public void pushNext(T next, boolean isLast);

}
