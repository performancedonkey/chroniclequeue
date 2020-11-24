package algoApi;

public interface Batcher<T> {

    public void pushNext(T next, boolean isLast);

}
