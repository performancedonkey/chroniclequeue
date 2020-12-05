package chronicle;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class TestBook {

    public static void main(String[] args) {
        ArrayList<Float> layers = new ArrayList<Float>();
        int ix = 313;
        float value = 1594f;
        layers.ensureCapacity(ix + 1);
        layers.add(ix, value);
        System.out.println(layers);
    }
}
