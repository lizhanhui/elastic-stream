package apis.exception;

import java.io.Serial;

/**
 * The exception thrown when an error occurs while writing to the storage.
 * <p>
 * It's the base class for all storage exceptions.
 */
public class ClientException extends Exception {

    @Serial private static final long serialVersionUID = 6338133491680948104L;

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }

    public ClientException() {
        super();
    }

}
