package SparkPinkMST;

import java.io.Serializable;
import java.util.Arrays;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by cjin on 4/10/14.
 */
public class Point implements Cloneable, Serializable, KryoSerializable{

    private static final long serialVersionUID = 1L;

    protected int id;
    protected Double[] coords;

    public Point() {
        this.id = 0;
        this.coords = null;
    }

    public Point(Point other) {
        this.id = other.id;
        this.coords = other.coords.clone();
    }

    public Point(int id, Double[] arr) {
        this.id = id;
        this.coords = arr.clone();
    }

    public void set(int id, Double[] arr) {
        this.id = id;
        this.coords = arr.clone();
    }

    @Override
    public Point clone() throws CloneNotSupportedException{
        return (Point)super.clone();
    }

    public void setCoords(Double[] arr) {
        this.coords = arr.clone();
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getDimension() {
        return coords.length;
    }

    public Double[] getCoords() {
        return coords;
    }

    public int getId() {
        return id;
    }

    public double distanceTo(Point other) {
        if (other.id == this.id) {
            return 0;
        }
        double d = 0;
        for (int i = 0; i < coords.length; i++) {
            d += Math.pow(coords[i] - other.coords[i], 2);
        }

        return Math.sqrt(d);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Point)) {
            throw new RuntimeException("other has wrong type");
        }

        Point otherP = (Point) other;
        if (id != otherP.id) {
            return false;
        }
        return Arrays.equals(coords, (otherP.coords));
    }

    @Override
    public String toString() {
        return id + ": " +  Arrays.toString(coords);
    }

    @Override
    public int hashCode() {
        double sum = 0;
        for (double v : coords) {
            sum += v;
        }
        return id * 163 + coords.length * 57 + (int) sum;
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.id = input.readInt(true);
        this.coords = (Double[]) kryo.readClassAndObject(input);
    }

    @Override
    public void write(Kryo kryo, Output output) {
         output.writeInt(id, true);
         kryo.writeClassAndObject(output, coords);
    }
}
