package net.joshdevins.talks.hadoopstart.mr;

/**
 * Domain object encapsulating an Apache access log line with the "combined" pattern/format.
 * 
 * @author Josh Devins
 */
public final class ApacheCombinedAccessLogEntry {

    private String remoteAddr;

    private String remoteLogname;

    private String user;

    private String timestamp;

    private String method;

    private String uri;

    private String protocol;

    private int statusCode;

    private long bytes;

    private String referrer;

    private String userAgent;

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteLogname(String remoteLogname) {
        this.remoteLogname = remoteLogname;
    }

    public String getRemoteLogname() {
        return remoteLogname;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public long getBytes() {
        return bytes;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getUserAgent() {
        return userAgent;
    }
}
