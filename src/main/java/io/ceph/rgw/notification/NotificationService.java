package io.ceph.rgw.notification;

import io.ceph.rgw.client.Clients;
import io.ceph.rgw.client.config.Configuration;
import io.ceph.rgw.client.config.RGWClientProperties;
import io.ceph.rgw.notification.elasticsearch.ESClient;
import io.ceph.rgw.notification.netty.HttpServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * NotificationService provides a publish and subscribe mechanism for the object store modification events.
 * see Ceph Rados gateway <a href="https://docs.ceph.com/en/latest/radosgw/pubsub-module/">PubSub module</a> for more details.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/4/29.
 */
public class NotificationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationService.class);

    public static void main(String[] args) {
        Configuration config = null;
        String name = null;
        try {
            name = System.getProperty("notification.config.location");
            if (StringUtils.isBlank(name)) {
                name = "config/notification.properties";
                config = new Configuration(name);
            } else {
                config = new Configuration(new File(name));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load configuration from resource [{}].", name, e);
            System.exit(1);
        }
        try {
            Clients clients = Clients.create(RGWClientProperties.loadFromConfig(config));
            ESClient esClient = new ESClient(config.getSubConfig("notification.es"));
            HttpServer server = new HttpServer(config.getSubConfig("notification.http"), clients.getObject(), esClient);
            setDefaultUncaughtExceptionHandler();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.close();
                esClient.close();
                LOGGER.info("Notification service shutdown.");
            }));
            server.start();
        } catch (Exception e) {
            LOGGER.error("Failed to start service.", e);
            System.exit(1);
        }
    }

    private static void setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("Exception caught in {}.", t.getName(), e));
    }
}
