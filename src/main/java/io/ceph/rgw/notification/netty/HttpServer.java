package io.ceph.rgw.notification.netty;

import io.ceph.rgw.client.ObjectClient;
import io.ceph.rgw.client.config.Configuration;
import io.ceph.rgw.client.util.AbstractClosable;
import io.ceph.rgw.notification.elasticsearch.ESClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A http server based on netty.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/19.
 */
public class HttpServer extends AbstractClosable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);
    private final AtomicBoolean started;
    private final HttpProperties properties;
    private final ServerBootstrap bootstrap;
    private final EventLoopGroup boss;
    private final EventLoopGroup workers;
    private volatile ChannelFuture future;

    public HttpServer(Configuration config, ObjectClient objectClient, ESClient esClient) {
        this.started = new AtomicBoolean(false);
        this.properties = new HttpProperties(config);
        this.boss = new NioEventLoopGroup(1);
        this.workers = new NioEventLoopGroup();
        this.bootstrap = new ServerBootstrap();
        this.bootstrap.group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, properties.getBacklog())
                .childOption(ChannelOption.SO_TIMEOUT, properties.getSocketTimeout())
                .childOption(ChannelOption.TCP_NODELAY, properties.isNoDelay())
                .childOption(ChannelOption.SO_LINGER, properties.getLinger())
                .childOption(ChannelOption.SO_KEEPALIVE, properties.isKeepAlive())
                .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, properties.getConnectionTimeout())
                .childOption(ChannelOption.SO_REUSEADDR, properties.isReuseAddr())
                .childOption(ChannelOption.SO_SNDBUF, properties.getSendBuffer())
                .childOption(ChannelOption.SO_RCVBUF, properties.getReceiveBuffer())
                .childHandler(new HttpInitializer(properties, objectClient, esClient));
    }

    private boolean notStarted() {
        return started.compareAndSet(false, true);
    }

    public void start() {
        if (notStarted()) {
            try {
                future = bootstrap.bind(properties.getHost(), properties.getPort()).sync();
                LOGGER.info("HttpServer started.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("HttpServer interrupted while binding.", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void doClose() {
        if (future != null) {
            try {
                future.channel().close().sync();
            } catch (Throwable throwable) {
                LOGGER.error("Failed to close server future.", throwable);
            }
        }
        if (boss != null) {
            try {
                boss.shutdownGracefully().sync();
            } catch (Throwable throwable) {
                LOGGER.error("Failed to close boss group.", throwable);
            }
        }
        if (workers != null) {
            try {
                workers.shutdownGracefully().sync();
            } catch (Throwable throwable) {
                LOGGER.error("Failed to close worker group.", throwable);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
