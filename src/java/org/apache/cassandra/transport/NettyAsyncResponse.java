package org.apache.cassandra.transport;

import java.util.UUID;

import org.apache.cassandra.net.AsyncResponse;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyAsyncResponse extends AsyncResponse<Message.Response>
{
    protected static final Logger logger = LoggerFactory.getLogger(NettyAsyncResponse.class);
    private final Request request;
    private final ServerConnection connection;
    private final ChannelHandlerContext ctx;
    private UUID tracingId;

    public NettyAsyncResponse(ChannelHandlerContext ctx, ServerConnection connection, Request request)
    {
        this.request = request;
        this.connection = connection;
        this.ctx = ctx;
    }

    @Override
    public void respond()
    {
        Response response;
        if (transformer == null)
            response = new ResultMessage.Void();
        else
            response = transformer.transform(results);
        respond(response);
    }

    public void respond(Response response)
    {
        if (response != null) {
            response.setStreamId(request.getStreamId());
            response.attach(connection);
            connection.applyStateTransition(request.type, response.type);
            response.setTracingId(tracingId);
            logger.debug("Responding: " + response);
        }
        ctx.getChannel().write(response);
    }

    public void setTracingId(UUID tracingId)
    {
        this.tracingId = tracingId;
    }
}
