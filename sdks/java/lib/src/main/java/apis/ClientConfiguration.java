package apis;

import java.time.Duration;

public class ClientConfiguration {
    private final String placementManagerEndpoint;
    private final Duration connectionTimeout;
    /**
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private final Duration channelMaxIdleTime;
    private final int clientAsyncSemaphoreValue;

    ClientConfiguration(String endpoint, Duration connectionTimeout, Duration channelMaxIdleTime, int clientAsyncSemaphoreValue) {
        this.placementManagerEndpoint = endpoint;
        this.connectionTimeout = connectionTimeout;
        this.channelMaxIdleTime = channelMaxIdleTime;
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public static ClientConfigurationBuilder newBuilder() {
        return new ClientConfigurationBuilder();
    }

    public String getPlacementManagerEndpoint() {
        return placementManagerEndpoint;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getChannelMaxIdleTime() {
        return channelMaxIdleTime;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }
}
