package io.ceph.rgw.notification.netty;


import io.ceph.rgw.client.config.Configuration;

/**
 * Properties of {@link HttpServer}.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/20.
 */
public class HttpProperties {
    private final String host;
    private final Integer port;
    private final Integer backlog;
    private final Boolean noDelay;
    private final Integer linger;
    private final Boolean keepAlive;
    private final Integer connectionTimeout;
    private final Integer socketTimeout;
    private final Boolean reuseAddr;
    private final Integer sendBuffer;
    private final Integer receiveBuffer;
    private final Boolean enableGzip;

    public HttpProperties(Configuration config) {
        this.host = config.getString("host", "127.0.0.1");
        this.port = config.getInteger("port", 8080);
        this.backlog = config.getInteger("backlog", 128);
        this.noDelay = config.getBoolean("noDelay", Boolean.TRUE);
        this.linger = config.getInteger("linger");
        this.keepAlive = config.getBoolean("keepAlive", Boolean.TRUE);
        this.connectionTimeout = config.getInteger("connectionTimeout", 5000);
        this.socketTimeout = config.getInteger("socketTimeout", 5000);
        this.reuseAddr = config.getBoolean("reuseAddr", Boolean.FALSE);
        this.sendBuffer = config.getInteger("sendBuffer");
        this.receiveBuffer = config.getInteger("receiveBuffer");
        this.enableGzip = config.getBoolean("enableGzip", Boolean.TRUE);
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public Integer getBacklog() {
        return backlog;
    }

    public Boolean isNoDelay() {
        return noDelay;
    }

    public Integer getLinger() {
        return linger;
    }

    public Boolean isKeepAlive() {
        return keepAlive;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public Boolean isReuseAddr() {
        return reuseAddr;
    }

    public Integer getSendBuffer() {
        return sendBuffer;
    }

    public Integer getReceiveBuffer() {
        return receiveBuffer;
    }

    public Boolean isEnableGzip() {
        return enableGzip;
    }

    @Override
    public String toString() {
        return "NettyProperties{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", backlog=" + backlog +
                ", noDelay=" + noDelay +
                ", linger=" + linger +
                ", keepAlive=" + keepAlive +
                ", connectionTimeout=" + connectionTimeout +
                ", socketTimeout=" + socketTimeout +
                ", reuseAddr=" + reuseAddr +
                ", sendBuffer=" + sendBuffer +
                ", receiveBuffer=" + receiveBuffer +
                ", enableGzip=" + enableGzip +
                '}';
    }
}
