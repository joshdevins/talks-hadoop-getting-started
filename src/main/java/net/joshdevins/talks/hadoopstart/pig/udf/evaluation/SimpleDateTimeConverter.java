package net.joshdevins.talks.hadoopstart.pig.udf.evaluation;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.Validate;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A simple date-time converter allowing you to specify the input and output format using Joda Time's
 * {@link DateTimeFormat} patterns. Joda Time is used since it preserves time zones which the JDK's
 * {@link SimpleDateFormat} and {@link Date} classes do not.
 * 
 * @author Florin Duroiu
 * @author Josh Devins
 */
public class SimpleDateTimeConverter extends EvalFunc<String> {

    private static final String DEFAULT_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

    private final DateTimeFormatter inputFormatter;

    private final DateTimeFormatter outputFormatter;

    public SimpleDateTimeConverter(final String outputDateFormat) {
        this(DEFAULT_DATE_FORMAT, outputDateFormat);
    }

    public SimpleDateTimeConverter(final String inputDateFormat, final String outputDateFormat) {

        Validate.notEmpty(inputDateFormat, "Input date format is required");
        Validate.notEmpty(outputDateFormat, "Output date format is required");

        inputFormatter = DateTimeFormat.forPattern(inputDateFormat).withOffsetParsed();
        outputFormatter = DateTimeFormat.forPattern(outputDateFormat).withOffsetParsed();
    }

    @Override
    public String exec(final Tuple input) throws IOException {

        // just do nothing if there is no input
        // TODO: Is this valid?
        if (input == null || input.size() == 0) {
            return null;
        }

        // TODO: Output warning if the Tuple is larger than 1
        String timeStr = input.get(0).toString();

        try {
            DateTime time = inputFormatter.parseDateTime(timeStr);
            return time.toString(outputFormatter);

        } catch (Exception e) {
            System.err.println("Error parsing date-time: " + e.getMessage());
            return null;
        }
    }

    @Override
    public Schema outputSchema(final Schema input) {

        Schema.FieldSchema tokenFs = new Schema.FieldSchema("value", DataType.CHARARRAY);
        return new Schema(tokenFs);
    }
}
