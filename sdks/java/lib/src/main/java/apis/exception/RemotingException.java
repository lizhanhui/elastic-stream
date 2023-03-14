package apis.exception;

import java.io.Serial;

public class RemotingException extends Exception{
    @Serial private static final long serialVersionUID = 6338133491680948104L;

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(Throwable cause) {
        super(cause);
    }

    public RemotingException() {
        super();
    }
}
