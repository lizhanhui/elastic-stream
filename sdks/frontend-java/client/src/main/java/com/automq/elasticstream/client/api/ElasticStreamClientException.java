package com.automq.elasticstream.client.api;

import java.util.concurrent.ExecutionException;

/**
 * All stream client exceptions will list extends ElasticStreamClientException and list here.
 */
public class ElasticStreamClientException extends ExecutionException {
    private final int code;

    public ElasticStreamClientException(int code, String str) {
        super("code: " + code + ", " + str);
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
