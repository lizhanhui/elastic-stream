package client.netty;

import apis.ClientConfiguration;
import apis.exception.RemotingConnectException;
import apis.exception.RemotingSendRequestException;
import apis.exception.RemotingTimeoutException;
import client.InvokeCallback;
import client.RemotingClient;
import client.cache.StreamNameIdCache;
import client.cache.StreamRangeCache;
import client.common.ClientId;
import client.common.RemotingUtil;
import client.misc.ThreadFactoryImpl;
import client.protocol.RemotingItem;
import client.protocol.SbpFrame;
import client.route.Address;
import client.route.Endpoints;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    protected final ClientId clientId;
    protected final ClientConfiguration clientConfiguration;
    protected final Address pmAddress;
    protected final ExecutorService clientCallbackExecutor;
    private final StreamNameIdCache streamNameIdCache;
    private final StreamRangeCache streamRangeCache;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentMap<Address, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ClientHouseKeepingService"));

    public NettyClient(final ClientConfiguration clientConfiguration) {
        this(clientConfiguration, null);
    }

    public NettyClient(final ClientConfiguration clientConfiguration,
        final EventLoopGroup eventLoopGroup) {
        super(clientConfiguration.getClientAsyncSemaphoreValue());
        this.clientConfiguration = clientConfiguration;

        this.pmAddress = new Endpoints(clientConfiguration.getPlacementManagerEndpoint()).getAddresses().get(0);
        this.clientId = new ClientId();

        this.streamNameIdCache = new StreamNameIdCache();
        this.streamRangeCache = new StreamRangeCache();

        final long clientIdIndex = clientId.getIndex();
        this.clientCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("ClientCallbackWorker", clientIdIndex));

        this.eventLoopGroupWorker = Objects.requireNonNullElseGet(eventLoopGroup, () -> new NioEventLoopGroup(1,
            new ThreadFactoryImpl("ClientEventLoopGroupWorker", clientId.getIndex())));
    }

    @Override
    public Endpoints getEndpoints() {
        return new Endpoints(clientConfiguration.getPlacementManagerEndpoint());
    }

    @Override
    public ClientId getClientId() {
        return clientId;
    }

    @Override
    public void start() throws Exception {
        NettyEncoder sharedEncoder = new NettyEncoder();
        this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) clientConfiguration.getConnectionTimeout().toMillis())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        sharedEncoder,
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, (int) clientConfiguration.getChannelMaxIdleTime().getSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        TimerTask timerTaskScanResponseTable = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1, TimeUnit.SECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanResponseTable, 3, TimeUnit.SECONDS);
    }

    @Override
    public RemotingItem invokeSync(Address address, RemotingItem request,
        long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingConnectException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getOrCreateChannel(address);
        if (channel != null && channel.isActive()) {
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + address + "] timeout");
                }
                SbpFrame response = this.invokeSyncImpl(channel, (SbpFrame) request, timeoutMillis - costTime);
                updateChannelLastResponseTime(address);
                return response;
            } catch (RemotingSendRequestException | RemotingTimeoutException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", address);
                this.closeChannel(address, channel);
                throw e;
            }
        } else {
            this.closeChannel(address, channel);
            throw new RemotingConnectException(address);
        }
    }

    @Override
    public void invokeAsync(Address address, RemotingItem request, long timeoutMillis,
        InvokeCallback invokeCallback) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingConnectException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getOrCreateChannel(address);
        if (channel != null && channel.isActive()) {
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeAsync call the addr[" + address + "] timeout");
                }
                this.invokeAsyncImpl(channel, (SbpFrame) request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, address));
            } catch (RemotingSendRequestException | RemotingTimeoutException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", address);
                this.closeChannel(address, channel);
                throw e;
            }
        } else {
            this.closeChannel(address, channel);
            throw new RemotingConnectException(address);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.timer.stop();

            for (Address address : this.channelTables.keySet()) {
                this.closeChannel(address, this.channelTables.get(address).getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.clientCallbackExecutor != null) {
                this.clientCallbackExecutor.shutdown();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }
    }

    public void closeChannel(final Address address, final Channel channel) {
        if (null == channel) {
            return;
        }

        final Address addressRemote = null == address ? RemotingUtil.parseChannelRemoteAddress(channel) : address;
        assert addressRemote != null;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addressRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addressRemote, prevCW != null);

                    if (null == prevCW) {
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addressRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addressRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addressRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addressRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    Address addrRemote = null;
                    for (Map.Entry<Address, ChannelWrapper> entry : channelTables.entrySet()) {
                        Address key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    private Channel getOrCreateChannel(final Address address) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(address);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return createChannel(address);
    }

    private Channel createChannel(final Address address) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(address);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                cw = this.channelTables.get(address);
                if (cw != null) {

                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(address);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = bootstrap
                        .connect(address.getHost(), address.getPort());
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", address);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(address, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(clientConfiguration.getConnectionTimeout().toMillis())) {
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", address, channelFuture);
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + address + "] failed, " + channelFuture);
                }
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", address, clientConfiguration.getConnectionTimeout().toMillis(),
                    channelFuture);
            }
        }

        return null;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.clientCallbackExecutor;
    }

    private void updateChannelLastResponseTime(final Address addr) {
        Address address = addr;
        if (address == null) {
            address = this.pmAddress;
        }
        if (address == null) {
            log.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<SbpFrame> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, SbpFrame frame) throws Exception {
            processMessageReceived(ctx, frame);
        }
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        // affect sync or async request.
        private long lastResponseTime;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }
    }

    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final Address addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, Address addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            if (responseFuture != null && responseFuture.isSendRequestOK() && responseFuture.getResponseSbpFrame() != null) {
                NettyClient.this.updateChannelLastResponseTime(addr);
            }
            this.invokeCallback.operationComplete(responseFuture);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingUtil.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingUtil.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingUtil.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingUtil.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyClient.this.failFast(ctx.channel());
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent event) {
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingUtil.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingUtil.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
        }
    }
}
