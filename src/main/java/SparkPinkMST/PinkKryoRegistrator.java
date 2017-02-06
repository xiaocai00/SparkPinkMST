package SparkPinkMST;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
/**
 * Created by cjin on 4/12/14.
 */
public class PinkKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Point.class);
    }

}
