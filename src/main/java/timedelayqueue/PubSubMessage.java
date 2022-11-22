package timedelayqueue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PubSubMessage implements TimestampedObject {
    private UUID              id;
    private Timestamp         timestamp;
    private final String      content;
    private final boolean     isTransient;
    private final UUID        sender;
    private final List<UUID>  receiver;

    private static final UUID ZERO_UUID = new UUID(0l, 0l);

    public static final PubSubMessage NO_MSG =
            new PubSubMessage(ZERO_UUID, new Timestamp(0), ZERO_UUID, ZERO_UUID, "");

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Timestamp getTimestamp() {
        return (Timestamp) timestamp.clone();
    }

    // create a PubSubMessage instance with explicit args;
    // content should be in JSON format to accommodate a variety of
    // message types (e.g., TweetData)
    public PubSubMessage(UUID id, Timestamp timestamp,
                          UUID sender, UUID receiver, String content) {
        this.id          = id;
        this.timestamp   = timestamp;
        this.sender      = sender;
        this.isTransient = false;
        this.content     = content;
        this.receiver    = new ArrayList<>();
        this.receiver.add(receiver);
    }

    // create a PubSubMessage instance with explicit args
    // a message may be intended for more than one user
    public PubSubMessage(UUID id, Timestamp timestamp,
                          UUID sender, List<UUID> receiver, String content) {
        this.id          = id;
        this.timestamp   = timestamp;
        this.sender      = sender;
        this.receiver    = new ArrayList<>(receiver);
        this.isTransient = false;
        this.content     = content;
    }

    // create a PubSubMessage instance with implicit args
    public PubSubMessage(UUID sender, UUID receiver, String content) {
        this(
                UUID.randomUUID(),
                new Timestamp(System.currentTimeMillis()),
                sender, receiver,
                content
        );
    }

    // create a PubSubMessage instance with implicit args
    public PubSubMessage(UUID sender, List<UUID> receiver, String content) {
        this(
            UUID.randomUUID(),
            new Timestamp(System.currentTimeMillis()),
            sender, receiver,
            content
        );
    }

    // obtain message content
    // note that this will be in JSON format
    public String getContent() {
        return content;
    }

    // is the message transient?
    // default is false
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PubSubMessage) {
            PubSubMessage that = (PubSubMessage) other;
            return this.id.equals(that.id);
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("%s: (%s) %s", id.toString(), timestamp.toString(), content);
    }

}