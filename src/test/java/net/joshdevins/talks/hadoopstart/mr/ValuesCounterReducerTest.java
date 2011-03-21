package net.joshdevins.talks.hadoopstart.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class ValuesCounterReducerTest {

    private ValuesCounterReducer reducer;

    private ReduceDriver<Text, NullWritable, NullWritable, Text> driver;

    @Before
    public void before() {
        reducer = new ValuesCounterReducer();
        driver = new ReduceDriver<Text, NullWritable, NullWritable, Text>(reducer);
    }

    @Test
    public void testMultipleValues() {
        test(10);
    }

    @Test
    public void testNoValues() {
        // should never happen, but whatever
        test(0);
    }

    @Test
    public void testSingleValue() {
        test(1);
    }

    private void test(final int count) {

        driver.setInputKey(new Text("key"));

        for (int i = 0; i < count; i++) {
            driver.addInputValue(NullWritable.get());
        }

        driver.addOutput(NullWritable.get(), new Text("key," + count));

        driver.runTest();
    }
}
