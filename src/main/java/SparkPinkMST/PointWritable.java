package SparkPinkMST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.hadoop.io.Writable;
/**
 * Created by cjin on 4/10/14.
 */
@Nonnull
public final class PointWritable extends Point implements Writable{

    private static final long serialVersionUID = 1L;

    public PointWritable() {
        super();
    }

    public PointWritable(Point other) {
        super(other.id, other.coords);
    }

    public PointWritable(PointWritable other) {
        super();
    }

    public PointWritable(int id, Double[] coords) {
        super(id, coords.clone());
    }

    double distanceTo(PointWritable otherP) {
        return super.distanceTo(otherP);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        coords = new Double[in.readInt()];
        for (int i = 0; i < coords.length; i++) {
            coords[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        if (coords == null) {
            throw new RuntimeException("coords is null");
        }
        out.writeInt(coords.length);
        for (double v : coords) {
            out.writeDouble(v);
        }
    }

    @Override
    public PointWritable clone() throws CloneNotSupportedException{
        return (PointWritable) super.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PointWritable)) {
            throw new RuntimeException("other has wrong type");
        }
        PointWritable otherP = (PointWritable) other;
        if (id == otherP.id) {
            for (int i = 0; i < coords.length; i++) {
                if (coords[i] != otherP.coords[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        String ret = "" + id;
        if (coords != null) {
            for (double v : coords) {
                ret += String.format(" %.2f", v);
            }
        }
        return ret;
    }

    @Override
    public int hashCode() {
        double sum = 0;
        for (double v : coords) {
            sum += v;
        }
        return id * 163 + coords.length * 57 + (int) sum;
    }

}
