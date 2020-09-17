package io.ceph.rgw.notification.model;

/**
 * Object event type.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/13.
 */
public enum Event {
    OBJECT_CREATE,
    OBJECT_DELETE,
    DELETE_MARKER_CREATE,
    UNKNOWN_EVENT
}
