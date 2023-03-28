package sdk.elastic.storage.client.kv;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import sdk.elastic.storage.apis.manager.KvManager;
import sdk.elastic.storage.client.cache.CommonCache;
import sdk.elastic.storage.client.route.Address;
import sdk.elastic.storage.grpc.kv.ErrorType;
import sdk.elastic.storage.grpc.kv.LoadResponse;
import sdk.elastic.storage.grpc.kv.StoreResponse;

public class KvCache {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(KvCache.class);
    private static final int DEFAULT_CACHE_SIZE = 200;
    private final ManagedChannel channel;
    private final KvManager kvManager;
    private final CommonCache<String, ByteString> cache;

    /**
     * Create a new KvCache with a default CacheLoader offered by KvManager.
     *
     * @param size    the maximum number of entries the cache may contain
     * @param address the address of the PM
     */
    public KvCache(int size, Address address) {
        this(Grpc.newChannelBuilder(address.getAddress(), InsecureChannelCredentials.create())
            .build(), size);
    }

    /**
     * Create a new KvCache with a default CacheLoader offered by KvManager.
     *
     * @param address the address of the PM
     */
    public KvCache(Address address) {
        this(DEFAULT_CACHE_SIZE, address);
    }

    protected KvCache(ManagedChannel channel, int size) {
        this(channel, new KvManagerImpl(channel), size);
    }

    protected KvCache(ManagedChannel channel, KvManager kvManager, int size) {
        this.channel = channel;
        this.kvManager = kvManager;
        this.cache = new CommonCache<String, ByteString>(size, new CacheLoader<String, ByteString>() {
            @Override
            public ByteString load(String key) throws ExecutionException, InterruptedException {
                return Futures.transform(kvManager.load(Collections.singletonList(key).iterator(), null),
                    (LoadResponse response) -> {
                        if (response.getItemsCount() == 0 || response.getItems(0).getError().getType() != ErrorType.OK) {
                            log.error("Failed to load key {}, itemCount {}, errorType {}, errorMessage {}", key,
                                response.getItemsCount(), response.getItems(0).getError().getType(),
                                response.getItems(0).getError().getMessage());
                            return null;
                        }
                        return response.getItems(0).getPayload();
                    }, MoreExecutors.directExecutor()).get();
            }
        }) {};
    }

    public void shutdown() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    /**
     * Get the value of the key.
     *
     * @param key the key to get
     * @return the value of the key
     */
    public ByteBuffer get(String key) {
        return cache.get(key).asReadOnlyByteBuffer();
    }

    /**
     * Store the key-value pair.
     * If the key or value is null or empty, the operation will be ignored.
     *
     * @param key   the key to store
     * @param value the value to store
     */
    public void put(String key, ByteBuffer value) {
        if (key == null || key.isEmpty() || value == null || value.remaining() == 0) {
            return;
        }
        Futures.addCallback(kvManager.save(Collections.singletonMap(key, value.duplicate()), null),
            new FutureCallback<StoreResponse>() {
                @Override
                public void onSuccess(StoreResponse response) {
                    cache.put(key, ByteString.copyFrom(value.duplicate()));
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to store key {}", key, t);
                }
            }, MoreExecutors.directExecutor());
    }

    /**
     * Delete the key-value pair.
     *
     * @param key the key to delete
     */
    public void delete(String key) {
        if (key == null) {
            return;
        }
        Futures.addCallback(kvManager.delete(Collections.singletonList(key).iterator(), null),
            new FutureCallback<StoreResponse>() {
                @Override
                public void onSuccess(StoreResponse response) {
                    cache.invalidate(key);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to delete key {}", key, t);
                }
            }, MoreExecutors.directExecutor());
    }

    /**
     * Get the size of the cache.
     *
     * @return the size of the cache
     */
    public long getSize() {
        return cache.getSize();
    }
}
