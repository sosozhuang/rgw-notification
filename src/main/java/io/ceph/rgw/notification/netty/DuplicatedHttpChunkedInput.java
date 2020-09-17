package io.ceph.rgw.notification.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.stream.ChunkedStream;

/**
 * A HttpChunkedInput that can be sent to multiple channels.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/29.
 */
public class DuplicatedHttpChunkedInput extends HttpChunkedInput implements ByteBufHolder {
    private final ByteBuf data;
    private final int chunkSize;

    public DuplicatedHttpChunkedInput(ByteBuf data) {
        super(new ChunkedStream(new ByteBufInputStream(data)), null);
        this.data = data;
        this.chunkSize = 8192;
    }

    public DuplicatedHttpChunkedInput(ByteBuf data, int chunkSize) {
        super(new ChunkedStream(new ByteBufInputStream(data), chunkSize), null);
        this.data = data;
        this.chunkSize = chunkSize;
    }

    @Override
    public ByteBuf content() {
        return data;
    }

    @Override
    public ByteBufHolder copy() {
        return replace(data.copy());
    }

    @Override
    public ByteBufHolder duplicate() {
        return replace(data.duplicate());
    }

    @Override
    public ByteBufHolder retainedDuplicate() {
        return replace(data.retainedDuplicate());
    }

    @Override
    public ByteBufHolder replace(ByteBuf content) {
        return new DuplicatedHttpChunkedInput(content, chunkSize);
    }

    @Override
    public int refCnt() {
        return data.refCnt();
    }

    @Override
    public ByteBufHolder retain() {
        data.retain();
        return this;
    }

    @Override
    public ByteBufHolder retain(int increment) {
        data.retain(increment);
        return this;
    }

    @Override
    public ByteBufHolder touch() {
        data.touch();
        return this;
    }

    @Override
    public ByteBufHolder touch(Object hint) {
        data.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return data.release();
    }

    @Override
    public boolean release(int decrement) {
        return data.release(decrement);
    }
}
