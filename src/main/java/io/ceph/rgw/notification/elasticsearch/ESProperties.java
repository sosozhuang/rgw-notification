package io.ceph.rgw.notification.elasticsearch;

import io.ceph.rgw.client.config.Configuration;

import java.util.List;

/**
 * Properties of {@link ESClient}.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/20.
 */
public class ESProperties {
    private final List<String> hosts;
    private final String scheme;
    private final String username;
    private final String password;
    private final String caPath;
    private final String keyPath;
    private final String keyPass;
    private final String certPath;
    private final Integer maxConnections;
    private final Integer socketTimeout;
    private final Integer connectionTimeout;
    private final Integer connectionRequestTimeout;
    private final String index;

    public ESProperties(Configuration config) {
        this.hosts = config.getStrings("hosts");
        this.scheme = config.getString("scheme", "http");
        this.username = config.getString("username");
        this.password = config.getString("password");
        this.caPath = config.getString("caPath");
        this.keyPath = config.getString("keyPath");
        this.keyPass = config.getString("keyPass");
        this.certPath = config.getString("certPath");
        this.maxConnections = config.getInteger("maxConnections", 0);
        this.socketTimeout = config.getInteger("socketTimeout", -1);
        this.connectionTimeout = config.getInteger("connectionTimeout", -1);
        this.connectionRequestTimeout = config.getInteger("connectionRequestTimeout", -1);
        this.index = config.getString("index");
    }

    public List<String> getHosts() {
        return hosts;
    }

    public String getScheme() {
        return scheme;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getCaPath() {
        return caPath;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public String getKeyPass() {
        return keyPass;
    }

    public String getCertPath() {
        return certPath;
    }

    public Integer getMaxConnections() {
        return maxConnections;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public String getIndex() {
        return index;
    }
}
