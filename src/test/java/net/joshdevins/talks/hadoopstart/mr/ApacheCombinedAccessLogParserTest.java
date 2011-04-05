package net.joshdevins.talks.hadoopstart.mr;

import junit.framework.Assert;

import org.junit.Test;

public class ApacheCombinedAccessLogParserTest {

    @Test
    public void testParseInvalidLine() throws Exception {

        ApacheCombinedAccessLogEntry entry = ApacheCombinedAccessLogParser.parse("foobar");
        Assert.assertNull(entry);
    }

    @Test
    public void testParseValidLine() throws Exception {

        ApacheCombinedAccessLogEntry entry = ApacheCombinedAccessLogParser
                .parse("1.2.3.4 - - [30/Sep/2008:15:07:53 -0400] \"GET / HTTP/1.1\" 200 3190 \"-\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"");

        Assert.assertNotNull(entry);
        Assert.assertEquals("1.2.3.4", entry.getRemoteAddr());
        Assert.assertEquals("-", entry.getRemoteLogname());
        Assert.assertEquals("-", entry.getUser());
        Assert.assertEquals("30/Sep/2008:15:07:53 -0400", entry.getTimestamp());
        Assert.assertEquals("GET", entry.getMethod());
        Assert.assertEquals("/", entry.getUri());
        Assert.assertEquals("HTTP/1.1", entry.getProtocol());
        Assert.assertEquals(200, entry.getStatusCode());
        Assert.assertEquals(3190L, entry.getBytes());
        Assert.assertEquals("-", entry.getReferrer());
        Assert.assertEquals(
                "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1",
                entry.getUserAgent());
    }

    @Test
    public void testParseValidLine_NoBytes() {

        ApacheCombinedAccessLogEntry entry = ApacheCombinedAccessLogParser
                .parse("1.2.3.4 - - [30/Sep/2008:15:07:53 -0400] \"GET / HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"");

        Assert.assertNotNull(entry);
        Assert.assertEquals(0L, entry.getBytes());
    }
}
