package apis.manager;

import java.util.concurrent.CompletableFuture;

public interface ResourceManager {
    CompletableFuture<String> createStreams();
}
