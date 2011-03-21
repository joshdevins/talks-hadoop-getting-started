package net.joshdevins.talks.hadoopstart.pig.udf.evaluation;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PercentileTest {

    private Percentile function;

    private DefaultTuple inputTuple;

    private DataBag dataBag;

    @Before
    public void before() throws Exception {

        function = new Percentile();
        inputTuple = new DefaultTuple();
        dataBag = new DefaultDataBag();

        inputTuple.append(dataBag);
    }

    @Test
    public void test90Percentile() throws Exception {
        testFunction(90D, new Integer[] { 1, 2, 3, 4, 5, 7, 8, 9, 10 }, 10);
    }

    @Test
    public void testMedianInBagWithNanValue() throws Exception {
        testFunction(50D, new Double[] { 0D, 1D, 2D, 3D, 4D, Double.NaN }, 2.5);
    }

    @Test
    public void testMedianInSmallBag() throws Exception {
        testFunction(50D, new Double[] { 0D, 1D, 30D, 100D, 10D }, 10D);
    }

    @Test
    public void testMedianInTinyBag() throws Exception {
        testFunction(50D, new Double[] { 0D, 1D }, 0.5);
    }

    private void buildDataBag(final Object... numbers) {

        for (Object number : numbers) {

            Tuple tuple = new DefaultTuple();
            tuple.append(number);

            dataBag.add(tuple);
        }
    }

    private void testFunction(final double quantile, final Object[] numbers, final double expectedValue)
            throws Exception {

        inputTuple.append(quantile);
        buildDataBag(numbers);

        double actual = function.exec(inputTuple);
        Assert.assertEquals(expectedValue, actual, 0D);
    }
}
