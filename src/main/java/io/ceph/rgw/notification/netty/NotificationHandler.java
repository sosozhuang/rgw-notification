package io.ceph.rgw.notification.netty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.ceph.rgw.client.ObjectClient;
import io.ceph.rgw.client.action.ActionListener;
import io.ceph.rgw.client.model.GetObjectInfoResponse;
import io.ceph.rgw.client.model.Metadata;
import io.ceph.rgw.client.model.notification.ObjectInfo;
import io.ceph.rgw.client.model.notification.ObjectMetadataInfo;
import io.ceph.rgw.notification.elasticsearch.ESClient;
import io.ceph.rgw.notification.model.ObjectEvent;
import io.ceph.rgw.notification.model.ObjectEvents;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Accepts publish notifications from Ceph Rados gateway.
 * also subscribe requests from {@link io.ceph.rgw.client.SubscribeClient}.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/19.
 */
@ChannelHandler.Sharable
public class NotificationHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationHandler.class);
    private static final byte[] INTRO = "Rados gateway notification broadcasting service.".getBytes(CharsetUtil.UTF_8);
    private static final byte[] LINE_SEPARATOR = "\r\n".getBytes(CharsetUtil.US_ASCII);
    private static final ExpressionParser PARSER;
    private static final ChannelGroup SUB_CHANNELS;
    private static final AttributeKey<Expression> EXPRESSION_KEY;
    private static final AttributeKey<StandardEvaluationContext> CONTEXT_KEY;
    private static final ObjectMapper MAPPER;
    private final ESClient esClient;
    private final ObjectClient objectClient;
    private final ByteBuf content;
    private final ByteBuf lineSep;

    static {
        PARSER = new SpelExpressionParser();
        SUB_CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EXPRESSION_KEY = AttributeKey.newInstance("expression");
        CONTEXT_KEY = AttributeKey.newInstance("context");
        MAPPER = new ObjectMapper();
        MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
                .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS'Z'"));
    }

    NotificationHandler(ObjectClient objectClient, ESClient esClient) {
        this.objectClient = Objects.requireNonNull(objectClient);
        this.esClient = Objects.requireNonNull(esClient);
        this.content = PooledByteBufAllocator.DEFAULT.heapBuffer(INTRO.length, INTRO.length);
        this.content.writeBytes(INTRO);
        this.lineSep = PooledByteBufAllocator.DEFAULT.heapBuffer(LINE_SEPARATOR.length, LINE_SEPARATOR.length);
        this.lineSep.writeBytes(LINE_SEPARATOR);
    }

    private static ByteBuf writeContent(ChannelHandlerContext ctx, String content) {
        return ctx.alloc().buffer().writeBytes(content.getBytes());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (!request.decoderResult().isSuccess()) {
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Request decode error")));
            return;
        }
        URI uri = new URI(request.uri());
        String p = uri.getPath();
        switch (p) {
            case "/publish":
                handlePublish(ctx, request);
                break;
            case "/subscribe":
                handleSubscribe(ctx, request, uri);
                break;
            case "/":
                DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content.retainedDuplicate());
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
                sendHttpResponse(ctx, request, response);
                break;
            default:
                sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Unsupported uri path")));
        }
    }

    private void handlePublish(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (request.method() != HttpMethod.POST) {
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED));
            return;
        }
        byte[] content = ByteBufUtil.getBytes(request.content());
        try {
            ObjectEvents events = MAPPER.readValue(content, ObjectEvents.class);
            for (ObjectEvent event : events.getEvents()) {
                LOGGER.debug("Received object event [{}].", event);
                switch (event.getEvent()) {
                    case OBJECT_CREATE:
                        objectCreated(ctx, event);
                        break;
                    case OBJECT_DELETE:
                        objectDeleted(event);
                        break;
                    default:
                        LOGGER.info("Received object event type [{}].", event.getEvent());
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize content [{}].", new String(content), e);
        } finally {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE);
//            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            sendHttpResponse(ctx, request, response);
        }
    }

    private void objectCreated(ChannelHandlerContext ctx, ObjectEvent event) {
        ObjectInfo info = event.getInfo();
        objectClient.prepareGetObjectInfo()
                .withBucketName(info.getBucket().getName())
                .withKey(info.getKey().getName())
                .execute(new ActionListener<GetObjectInfoResponse>() {
                    @Override
                    public void onSuccess(GetObjectInfoResponse response) {
                        LOGGER.debug("Get object info: [{}].", response);
                        Metadata metadata = response.getMetadata();
                        ObjectMetadataInfo objectInfo = new ObjectMetadataInfo(info, metadata);
                        byte[] bytes;
                        try {
                            bytes = MAPPER.writeValueAsBytes(objectInfo);
                        } catch (JsonProcessingException e) {
                            LOGGER.error("Failed to serialize object info [{}].", objectInfo, e);
                            return;
                        }
                        esClient.insert(objectInfo.getInfo().getBucket().getName() + "." + objectInfo.getInfo().getKey().getName(), bytes);
                        ByteBuf message = ctx.alloc().heapBuffer(bytes.length, bytes.length)
                                .writeBytes(bytes);
                        bytes = Integer.toString(bytes.length).getBytes(CharsetUtil.US_ASCII);
                        ByteBuf len = ctx.alloc().heapBuffer(bytes.length, bytes.length)
                                .writeBytes(bytes);
                        CompositeByteBuf buf = new CompositeByteBuf(ctx.alloc(), false, 4);
                        buf.addComponents(true, len, lineSep.retainedDuplicate(), message, lineSep.retainedDuplicate());
                        DuplicatedHttpChunkedInput input = new DuplicatedHttpChunkedInput(buf);
                        SUB_CHANNELS.writeAndFlush(input, channel -> {
                            Expression expression = channel.attr(EXPRESSION_KEY).get();
                            StandardEvaluationContext context = channel.attr(CONTEXT_KEY).get();
                            context.setRootObject(metadata.all());
                            try {
                                return expression.getValue(context, Boolean.class);
                            } catch (EvaluationException e) {
                                return false;
                            }
                        }).addListener(NotificationHandler::logFailedListener);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        LOGGER.error("Failed to get object[{}]/[{}] info.", info.getBucket().getName(), info.getKey().getName(), cause);
                    }
                });
    }

    private void objectDeleted(ObjectEvent event) {
        ObjectInfo info = event.getInfo();
        esClient.delete(info.getBucket().getName(), info.getKey().getName());
    }

    private static void logFailedListener(Future<? super Void> future) throws Exception {
        if (future.isDone() && !future.isSuccess()) {
            LOGGER.error("Failed to notify client.", future.cause());
        }
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private void handleSubscribe(ChannelHandlerContext ctx, FullHttpRequest request, URI uri) {
        if (request.method() != HttpMethod.GET) {
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED));
            return;
        }
        Channel ch = ctx.channel();
        List<NameValuePair> pairs = URLEncodedUtils.parse(uri.getQuery(), UTF8);
        String condition = pairs.stream().filter(p -> "condition".equals(p.getName())).map(NameValuePair::getValue).findAny().orElse(null);
        if (StringUtils.isBlank(condition)) {
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Condition cannot be empty string")));
            return;
        }
        condition = new String(Base64.decodeBase64(condition));
        LOGGER.debug("Received subscription from [{}], condition [{}].", ch.remoteAddress(), condition);
        Expression expression;
        try {
            expression = PARSER.parseExpression(condition);
        } catch (ParseException e) {
            LOGGER.error("Failed to parse expression[{}].", condition, e);
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Failed to parse expression")));
            return;
        }
        StandardEvaluationContext context = new StandardEvaluationContext();
        try {
            context.setRootObject(Collections.emptyMap());
            Boolean value = expression.getValue(context, Boolean.class);
            if (value == null || value == Boolean.TRUE) {
                sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Invalid expression")));
                return;
            }
        } catch (UnsupportedOperationException e) {
            sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, writeContent(ctx, "Invalid expression")));
            return;
        } catch (SpelEvaluationException ignore) {
        }
        ch.attr(EXPRESSION_KEY).set(expression);
        ch.attr(CONTEXT_KEY).set(context);
        SUB_CHANNELS.add(ch);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED)
                .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        sendHttpResponse(ctx, request, response);
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response) {
        if (response instanceof FullHttpResponse) {
            response.headers().add(HttpHeaderNames.CONTENT_LENGTH, ((FullHttpResponse) response).content().readableBytes());
        }
        ChannelFuture f = ctx.channel().writeAndFlush(response);
        if (!HttpUtil.isKeepAlive(request) || response.status().code() >= 400) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("Exception caught in HttpHandler, remote address [{}].", ctx.channel().remoteAddress(), cause);
        ChannelFuture f = ctx.channel().writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR));
        f.addListener(ChannelFutureListener.CLOSE);
    }
}
