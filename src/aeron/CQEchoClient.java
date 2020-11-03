package aeron;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.NanoClock;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A mindlessly simple Echo client.
 */

public final class CQEchoClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(CQEchoClient.class);

    private static final int ECHO_STREAM_ID = 0x2044f002;

    private final MediaDriver media_driver;
    private final Aeron aeron;
    private final InetSocketAddress local_address;
    private final InetSocketAddress remote_address;

    private CQEchoClient(
            final MediaDriver in_media_driver,
            final Aeron in_aeron,
            final InetSocketAddress in_local_address,
            final InetSocketAddress in_remote_address) {
        this.media_driver =
                Objects.requireNonNull(in_media_driver, "media_driver");
        this.aeron =
                Objects.requireNonNull(in_aeron, "aeron");
        this.local_address =
                Objects.requireNonNull(in_local_address, "local_address");
        this.remote_address =
                Objects.requireNonNull(in_remote_address, "remote_address");
    }

    /**
     * Create a new client.
     *
     * @param media_directory The directory used for the underlying media driver
     * @param local_address   The local address used by the client
     * @param remote_address  The address of the server to which the client will connect
     * @return A new client
     * @throws Exception On any initialization error
     */

    public static CQEchoClient create(
            final Path media_directory,
            final InetSocketAddress local_address,
            final InetSocketAddress remote_address)
            throws Exception {
        Objects.requireNonNull(media_directory, "media_directory");
        Objects.requireNonNull(local_address, "local_address");
        Objects.requireNonNull(remote_address, "remote_address");

        final String directory =
                media_directory.toAbsolutePath().toString();

        final MediaDriver.Context media_context =
                new MediaDriver.Context()
                        .dirDeleteOnStart(true)
                        .aeronDirectoryName(directory);

        final Aeron.Context aeron_context =
                new Aeron.Context().aeronDirectoryName(directory);

        MediaDriver media_driver = null;

        try {
            media_driver = MediaDriver.launch(media_context);

            Aeron aeron = null;
            try {
                aeron = Aeron.connect(aeron_context);
            } catch (final Exception e) {
                closeIfNotNull(aeron);
                throw e;
            }

            return new CQEchoClient(media_driver, aeron, local_address, remote_address);
        } catch (final Exception e) {
            closeIfNotNull(media_driver);
            throw e;
        }
    }

    private static void closeIfNotNull(
            final AutoCloseable closeable)
            throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    public void run()
            throws Exception {
        try (final Subscription sub = this.setupSubscription()) {
            try (final Publication pub = this.setupPublication()) {
                this.runLoop(sub, pub);
            }
        }
    }

    private void runLoop(
            final Subscription sub,
            final Publication pub)
            throws InterruptedException {
        final UnsafeBuffer buffer =
                new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

        final Random random = new Random();

        /*
         * Try repeatedly to send an initial HELLO message
         */

        while (true) {
            if (pub.isConnected()) {
                if (sendMessage(pub, buffer, "HELLO " + this.local_address.getPort())) {
                    break;
                }
            }

            Thread.sleep(1000L);
        }

        /*
         * Send an infinite stream of random unsigned integers.
         */

        final FragmentHandler assembler =
                new FragmentAssembler(CQEchoClient::onParseMessage);

        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                if (pub.isConnected()) {
                    sendTs(pub, buffer, random.nextLong());
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                if (sub.isConnected()) {
                    sub.poll(assembler, 10);
                }
            }
        });
    }

    final static byte[] buf = new byte[40];

    private static void onParseMessage(
            final DirectBuffer buffer,
            final int startOfMessage,
            final int length,
            final Header header) {
        long arrival = NanoClock.getNanoTimeNow();
        buffer.getBytes(startOfMessage, buf);
        int offset = 0;
        int sequence = buffer.getInt(startOfMessage + offset);
        offset += 4;
        long sendingTime = buffer.getLong(startOfMessage + offset);
        offset += 8;
        long moreData = buffer.getLong(startOfMessage + offset);
        long ltcyNs = arrival - sendingTime;
//        final String response = new String(buffer.byteArray(), UTF_8);
        log.info(String.format("response: %d %d %d %d ", sequence, sendingTime, moreData, arrival));
    }

    private int sequence = 0;

    private boolean sendTs(
            final Publication pub,
            final UnsafeBuffer buffer,
            final Object payload) {
        long timestamp = NanoClock.getNanoTimeNow();
        int seq = sequence++;
        buffer.putInt(0, seq);
        buffer.putLong(4, timestamp);
        buffer.putLong(12, 1000000);

        int msgLength = 20;

        long result = 0L;
        for (int index = 0; index < 5; ++index) {
            result = pub.offer(buffer, 0, msgLength);
            if (result < 0L) {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            log.info(String.format("send: %d @ %d / %s", seq, timestamp, payload));
            return true;
        }

        log.error("could not send: {}", Long.valueOf(result));
        return false;
    }

    private static boolean sendMessage(
            final Publication pub,
            final UnsafeBuffer buffer,
            final String text) {
        log.info("send: {}", text);

        final byte[] value = text.getBytes(UTF_8);
        buffer.putBytes(0, value);

        long result = 0L;
        for (int index = 0; index < 5; ++index) {
            result = pub.offer(buffer, 0, text.length());
            if (result < 0L) {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            return true;
        }

        log.error("could not send: {}", Long.valueOf(result));
        return false;
    }

    private Publication setupPublication() {
        final String pub_uri =
                new ChannelUriStringBuilder()
                        .reliable(TRUE)
                        .media("udp")
                        .endpoint(this.remote_address.toString().replaceFirst("^/", ""))
                        .build();

        log.info("publication URI: {}", pub_uri);
        return this.aeron.addPublication(pub_uri, ECHO_STREAM_ID);
    }

    private Subscription setupSubscription() {
        final String sub_uri =
                new ChannelUriStringBuilder()
                        .reliable(TRUE)
                        .media("udp")
                        .endpoint(this.local_address.toString().replaceFirst("^/", ""))
                        .build();

        log.info("subscription URI: {}", sub_uri);
        return this.aeron.addSubscription(sub_uri, ECHO_STREAM_ID);
    }

    @Override
    public void close() {
        this.aeron.close();
        this.media_driver.close();
    }

    public static void main(
            final String[] args)
            throws Exception {
        DOMConfigurator.configure("./log4j.xml");

        if (args.length < 5) {
            log.error("usage: directory local-address local-port remote-address remote-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final InetAddress local_name = InetAddress.getByName(args[1]);
        final Integer local_port = Integer.valueOf(args[2]);
        final InetAddress remote_name = InetAddress.getByName(args[3]);
        final Integer remote_port = Integer.valueOf(args[4]);

        final InetSocketAddress local_address =
                new InetSocketAddress(local_name, local_port.intValue());
        final InetSocketAddress remote_address =
                new InetSocketAddress(remote_name, remote_port.intValue());

        try (final CQEchoClient client = create(directory, local_address, remote_address)) {
            client.run();
        }

    }
}
