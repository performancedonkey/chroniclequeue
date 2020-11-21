package disrupcher;

import com.lmax.disruptor.EventFactory;

public class HolderFactory<T> implements EventFactory<EventHolder<T>> {

    @Override
    public EventHolder<T> newInstance() {
        return new EventHolder<>();
    }
}
