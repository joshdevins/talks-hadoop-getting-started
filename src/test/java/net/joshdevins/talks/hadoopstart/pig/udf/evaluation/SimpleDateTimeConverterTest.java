package net.joshdevins.talks.hadoopstart.pig.udf.evaluation;

import java.io.IOException;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleDateTimeConverterTest {

    private final Tuple inputTuple = new DefaultTuple();

    @Test
    public void apacheCommonLogFormat() throws IOException {

        SimpleDateTimeConverter sdtc = new SimpleDateTimeConverter("dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss Z");
        String out = sdtc.exec(inputTuple);
        Assert.assertEquals("2010-12-01 01:00:05 +0000", out);
    }

    @Test
    public void formatHourlyInput() throws IOException {

        SimpleDateTimeConverter sdtc = new SimpleDateTimeConverter("yyyy-MM-dd HH");
        String out = sdtc.exec(inputTuple);
        Assert.assertEquals(out, "2010-12-01 01");
    }

    @Test
    public void formatMonthlyInput() throws IOException {

        SimpleDateTimeConverter sdtc = new SimpleDateTimeConverter("yyyy-MM");
        String out = sdtc.exec(inputTuple);
        Assert.assertEquals(out, "2010-12");
    }

    @Test
    public void formatStrangeInput() throws IOException {

        Tuple tuple = new DefaultTuple();
        tuple.append("1.Dec.2010 1 +0000");

        SimpleDateTimeConverter sdtc = new SimpleDateTimeConverter("dd.MMM.yyyy HH Z", "yyyy-MM-dd HH:mm:ss Z");
        String out = sdtc.exec(tuple);
        Assert.assertEquals(out, "2010-12-01 01:00:00 +0000");
    }

    @Before
    public void setup() {
        inputTuple.append("01/Dec/2010:01:00:05 +0000");
    }
}
