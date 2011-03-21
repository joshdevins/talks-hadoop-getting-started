package net.joshdevins.talks.hadoopstart.mr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses an Apache access log using the "combined" pattern. This one is simple since it fits a regular expression
 * nicely.
 * 
 * @author Josh Devins
 */
public final class ApacheCombinedAccessLogParser {

    // 1.2.3.4 - - [30/Sep/2008:15:07:53 -0400] "GET / HTTP/1.1" 200 3190 "-"
    // "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1"
    private final static String REGEX = "^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+\"(\\S+)\\s+(.+?)\\s+(HTTP[^\"]+)\"\\s+(\\S+)\\s+(\\S+)\\s+\"([^\"]*)\"\\s+\"(.*)\"$";

    private final static Pattern PATTERN = Pattern.compile(REGEX);

    private ApacheCombinedAccessLogParser() {
        throw new UnsupportedOperationException();
    }

    public static ApacheCombinedAccessLogEntry parse(final String raw) {

        Matcher matcher = PATTERN.matcher(raw);

        if (!matcher.matches()) {
            return null;
        }

        ApacheCombinedAccessLogEntry entry = new ApacheCombinedAccessLogEntry();
        entry.setRemoteAddr(matcher.group(1));
        entry.setRemoteLogname(matcher.group(2));
        entry.setUser(matcher.group(3));
        entry.setTimestamp(matcher.group(4));
        entry.setMethod(matcher.group(5));
        entry.setUri(matcher.group(6));
        entry.setProtocol(matcher.group(7));
        entry.setStatusCode(Integer.parseInt(matcher.group(8)));
        entry.setBytes(Long.parseLong(matcher.group(9)));
        entry.setReferrer(matcher.group(10));
        entry.setUserAgent(matcher.group(11));

        return entry;
    }
}
