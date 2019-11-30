package com.ververica.flinktraining.project;

import net.sf.javaml.core.*;
import net.sf.javaml.tools.InstanceTools;
import net.sf.javaml.tools.data.FileHandler;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import static org.junit.Assert.assertNotEquals;

public class EarthquakeBatchProjectExerciseTest {

    @Test
    public void ml() throws IOException {
        /* values of the attributes. */
        double[] values = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        /*
         * The simplest incarnation of the DenseInstance constructor will only
         * take a double array as argument an will create an instance with given
         * values as attributes and no class value set. For unsupervised machine
         * learning techniques this is probably the most convenient constructor.
         */
        Instance instance = new DenseInstance(values, "positive");

        /*
         * Here we will create an instance with 10 attributes, but will only set
         * the attributes with index 1,3 and 7 with a value.
         */
        /* Create instance with 10 attributes */
        Instance instance2 = new SparseInstance(10);
        /* Set the values for particular attributes */
        instance2.put(1, 1.0);
        instance2.put(3, 2.0);
        instance2.put(7, 4.0);

        Dataset data = new DefaultDataset();
        for (int i = 0; i < 10; i++) {
            Instance tmpInstance = InstanceTools.randomInstance(25);
            data.add(tmpInstance);
        }

        Dataset data2 = FileHandler.loadDataset(new File("output/batch/magnitudeType.csv"), 3, ";");
        System.out.println(data2);
    }

    @Test
    public void main() {
        assertNotEquals(new Tuple2<>(1, 2), new Tuple2<>(5, 6));
    }

    @Test
    public void main2() {
        assertNotEquals(" ", String.format(Locale.US, "%s;%d;%f;%s;", "value.f0, value.f1, value.f2, value.f3", 34, 0.123, new Boolean(true)));
    }
}
