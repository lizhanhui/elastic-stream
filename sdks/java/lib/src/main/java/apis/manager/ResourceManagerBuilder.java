package apis.manager;

public interface ResourceManagerBuilder {
    /**
     * Build ResourceManager.
     *
     * @return ResourceManager instance.
     * @throws Exception throws if building ResourceManager failed.
     */
    ResourceManager build() throws Exception;
}
