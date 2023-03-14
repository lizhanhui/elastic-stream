package client;

import apis.exception.RemotingConnectException;
import apis.exception.RemotingSendRequestException;
import apis.exception.RemotingTimeoutException;
import client.InvokeCallback;
import client.common.ClientId;
import client.protocol.RemotingItem;
import client.route.Address;
import client.route.Endpoints;
import java.io.Closeable;

public interface RemotingClient extends Closeable {
    /**
     * Retrieve Endpoints Information
     *
     * @return the endpoints associated with this client.
     */
    Endpoints getEndpoints();

    /**
     * Get Unique Client Identifier
     *
     * <p>Get the unique client identifier for each client.
     *
     * @return a unique client identifier.
     */
    public ClientId getClientId();

    /**
     * Start a client.
     *
     * @throws Exception
     */
    void start() throws Exception;

    /**
     * Invokes a request synchronously.
     *
     * @param request       request to be sent.
     * @param timeoutMillis timeout in milliseconds.
     * @return the response.
     */
    RemotingItem invokeSync(Address address, RemotingItem request, long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingConnectException;

    /**
     * Invokes a request asynchronously.
     * @param request request to be sent.
     *
     */
    /**
     * Invokes a request asynchronously.
     *
     * @param request        request to be sent.
     * @param timeoutMillis  timeout in milliseconds.
     * @param invokeCallback callback to be invoked when the response is received.
     */
    void invokeAsync(Address address, RemotingItem request, long timeoutMillis,
        InvokeCallback invokeCallback) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingConnectException;
}
