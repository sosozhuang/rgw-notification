package io.ceph.rgw.notification.model;

import java.util.List;

/**
 * An object that contains {@link ObjectEvent}s.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/25.
 */
public class ObjectEvents {
    private final List<ObjectEvent> events;

    public ObjectEvents(List<ObjectEvent> events) {
        this.events = events;
    }

    public List<ObjectEvent> getEvents() {
        return events;
    }

    @Override
    public String toString() {
        return "ObjectEvents{" +
                "events=" + events +
                '}';
    }
}
