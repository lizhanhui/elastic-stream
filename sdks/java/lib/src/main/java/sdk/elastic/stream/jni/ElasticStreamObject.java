package sdk.elastic.stream.jni;
public abstract class ElasticStreamObject implements AutoCloseable {
    static {
        System.loadLibrary("frontend_sdk");
    }
    long ptr;
}