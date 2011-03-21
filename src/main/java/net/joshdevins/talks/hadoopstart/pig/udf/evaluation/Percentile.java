package net.joshdevins.talks.hadoopstart.pig.udf.evaluation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.Validate;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A function for calculating percentiles from a {@link DataBag}. The input {@link DataBag} does not need to be
 * sorted.Input tuples should be of size two where the first field is the data bag to calculate on and the second is the
 * percentile to calculate.
 * 
 * @author Josh Devins
 */
public class Percentile extends EvalFunc<Double> {

    private final org.apache.commons.math.stat.descriptive.rank.Percentile percentile;

    public Percentile() {
        percentile = new org.apache.commons.math.stat.descriptive.rank.Percentile();
    }

    @Override
    public Double exec(final Tuple inputTuple) throws IOException {

        Validate.notNull(inputTuple, "Input tuple is null");
        Validate.isTrue(inputTuple.size() == 2, "Input tuple is not of size 2");

        Object field0 = inputTuple.get(0);
        Object field1 = inputTuple.get(1);

        Validate.isTrue(field0 instanceof DataBag, "Field 1 must be a bag");
        Validate.isTrue(field1 instanceof Double, "Field 2 must be a double describing the percentile to calculate");

        DataBag dataBag = (DataBag) field0;
        double quantile = (Double) field1;

        Validate.isTrue(dataBag.size() <= Integer.MAX_VALUE,
                "Data bag is too large to process, size must be less than Integer.MAX_VALUE");
        int size = Long.valueOf(dataBag.size()).intValue();

        // convert all of the data bag tuple fields to doubles
        double[] numbers = new double[size];
        Iterator<Tuple> iterator = dataBag.iterator();

        for (int i = 0; i < size; i++) {

            Tuple tuple = iterator.next();
            Validate.isTrue(tuple.size() == 1, "Inner tuple is not exactly size 1");
            Object innerField0 = tuple.get(0);

            if (innerField0 instanceof Integer) {
                numbers[i] = ((Integer) innerField0).doubleValue();
            } else if (innerField0 instanceof Long) {
                numbers[i] = ((Long) innerField0).doubleValue();
            } else if (innerField0 instanceof Double) {
                numbers[i] = (Double) innerField0;
            } else {
                throw new IllegalArgumentException("Field in tuple is not a long, integer or double");
            }
        }

        return percentile.evaluate(numbers, quantile);
    }

    @Override
    public Schema outputSchema(final Schema input) {

        Schema.FieldSchema tokenFs = new Schema.FieldSchema("value", DataType.DOUBLE);
        return new Schema(tokenFs);
    }
}
