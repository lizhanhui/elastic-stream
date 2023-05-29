package com.automq.elasticstream.client.api;

import java.util.concurrent.ExecutionException;

/**
 * All stream client exceptions will list extends ElasticStreamClientException and list here.
 */
public class ElasticStreamClientException extends ExecutionException {

    public static class ConnectionTimeout extends ElasticStreamClientException {
        ConnectionTimeout() {}
    }
    public static class ConnectionReset extends ElasticStreamClientException {
        private String addr;
        ConnectionReset(String addr) {
            this.addr = addr;
        }
    }
    public static class StreamNotFound extends ElasticStreamClientException {
        private long stream_id;
        StreamNotFound(long stream_id) {
            this.stream_id = stream_id;
        }
    }
    public static class BrokenChannel extends ElasticStreamClientException {
        private String str;
        BrokenChannel(String str) {
            this.str = str;
        }
    }
    public static class Internal extends ElasticStreamClientException {
        private String str;
        Internal(String str) {
            this.str = str;
        }
    } 

    // ReplicationError
    public static class ReplicationRpcTimeout extends ElasticStreamClientException {
        ReplicationRpcTimeout() {}
    }
    public static class ReplicationInternal extends ElasticStreamClientException {
        ReplicationInternal() {}
    }
    public static class ReplicationAlreadySealed extends ElasticStreamClientException {
        ReplicationAlreadySealed() {}
    }
    public static class ReplicationPreconditionRequired extends ElasticStreamClientException {
        ReplicationPreconditionRequired() {}
    }
    public static class ReplicationAlreadyClosed extends ElasticStreamClientException {
        ReplicationAlreadyClosed() {}
    }
    public static class ReplicationSealReplicaNotEnough extends ElasticStreamClientException {
        ReplicationSealReplicaNotEnough() {}
    }
    public static class ReplicationFetchOutOfRange extends ElasticStreamClientException {
        ReplicationFetchOutOfRange() {}
    }
    public static class ReplicationStreamNotExist extends ElasticStreamClientException {
        ReplicationStreamNotExist() {}
    }

    // ClientError
    public static class RpcClientErrorBadAddress extends ElasticStreamClientException {
        RpcClientErrorBadAddress() {}
    }
    public static class RpcClientErrorBadRequest extends ElasticStreamClientException {
        RpcClientErrorBadRequest() {}
    }
    public static class RpcClientErrorConnectionRefused extends ElasticStreamClientException {
        private String addr;
        RpcClientErrorConnectionRefused(String addr) {
            this.addr = addr;
        }
    }
    public static class RpcClientErrorConnectTimeout extends ElasticStreamClientException {
        private String addr;
        RpcClientErrorConnectTimeout(String addr) {
            this.addr = addr;
        }
    }
    public static class RpcClientErrorConnectFailure extends ElasticStreamClientException {
        private String addr;
        RpcClientErrorConnectFailure(String addr) {
            this.addr = addr;
        }
    }
    public static class RpcClientErrorDisableNagleAlgorithm extends ElasticStreamClientException {
        RpcClientErrorDisableNagleAlgorithm() {}
    }
    public static class RpcClientErrorChannelClosing extends ElasticStreamClientException {
        private String addr;
        RpcClientErrorChannelClosing(String addr) {
            this.addr = addr;
        }
    }
    public static class RpcClientErrorAppend extends ElasticStreamClientException {
        private int error_code;
        RpcClientErrorAppend(int error_code) {
            this.error_code = error_code;
        }
    }
    public static class RpcClientErrorCreateRange extends ElasticStreamClientException {
        private int error_code;
        RpcClientErrorCreateRange(int error_code) {
            this.error_code = error_code;
        }
    }
    public static class RpcClientErrorServerInternal extends ElasticStreamClientException {
        RpcClientErrorServerInternal() {}
    }
    public static class RpcClientErrorClientInternal extends ElasticStreamClientException {
        RpcClientErrorClientInternal() {}
    }
    public static class RpcClientErrorRpcTimeout extends ElasticStreamClientException {
        private long timeout_millis;
        RpcClientErrorRpcTimeout(long timeout_millis) {
            this.timeout_millis = timeout_millis;
        }
    }
}
