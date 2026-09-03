// Trimmed excerpt of clients/src/main/java/org/apache/kafka/common/protocol/Errors.java,
// kept only as a shape fixture for scripts/compat/__tests__/fetch.spec.js: a null-message
// constant with a lambda factory, a concatenated multi-line message, and normal entries.
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request.",
            UnknownServerException::new),
    NONE(0, null, message -> null),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is not within the range of offsets maintained by the server.",
            OffsetOutOfRangeException::new),
    NOT_LEADER_OR_FOLLOWER(6, "For requests intended only for the leader, this error indicates that the broker is not the current leader. " +
            "For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
            NotLeaderOrFollowerException::new),
    REBOOTSTRAP_REQUIRED(129, "Client metadata is stale. The client should rebootstrap to obtain new metadata.",
            RebootstrapRequiredException::new);

    private static final Logger log = LoggerFactory.getLogger(Errors.class);
    private static final Map<Class<?>, Errors> CLASS_TO_ERROR = new HashMap<>();
}
