package io.ceph.rgw.notification.model;


import io.ceph.rgw.client.model.notification.ObjectInfo;

import java.util.Date;

/**
 * Object event detail information.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/13.
 */
public class ObjectEvent {
    private final String id;
    private final Event event;
    private final Date timestamp;
    private final ObjectInfo info;

    public ObjectEvent(String id, Event event, Date timestamp, ObjectInfo info) {
        this.id = id;
        this.event = event;
        this.timestamp = timestamp;
        this.info = info;
    }

    public String getId() {
        return id;
    }

    public Event getEvent() {
        return event;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public ObjectInfo getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", event=" + event +
                ", timestamp=" + timestamp +
                ", info=" + info +
                '}';
    }
}
