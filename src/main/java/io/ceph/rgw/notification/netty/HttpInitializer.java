package io.ceph.rgw.notification.netty;

import io.ceph.rgw.client.ObjectClient;
import io.ceph.rgw.notification.elasticsearch.ESClient;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Objects;

/**
 * Assembles http handlers and adds {@link NotificationHandler} to channel's pipeline.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/19.
 */
public class HttpInitializer extends ChannelInitializer<SocketChannel> {
    private final HttpProperties properties;
    private final NotificationHandler handler;

    HttpInitializer(HttpProperties properties, ObjectClient objectClient, ESClient esClient) {
        this.properties = Objects.requireNonNull(properties);
        this.handler = new NotificationHandler(objectClient, esClient);
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        ChannelPipeline p = channel.pipeline();
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(65536));
        p.addLast(new HttpContentDecompressor());
        if (Boolean.TRUE.equals(properties.isEnableGzip())) {
            p.addLast(new HttpContentCompressor());
        }
        p.addLast(new ChunkedWriteHandler());
        p.addLast(handler);
    }
}
