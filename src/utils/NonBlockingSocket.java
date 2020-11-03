package utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;

public class NonBlockingSocket {
    protected static final Logger log = Logger.getLogger(NonBlockingSocket.class);
    private InetAddress group;
    private NetworkInterface networkInterface;
    private InetSocketAddress bindAddr;
    private boolean toCconnect;
    protected DatagramChannel socket;
    private MembershipKey key;
    ByteBuffer buffer;

    public NonBlockingSocket(String address, int port, NetworkInterface networkInterface) throws UnknownHostException {
        group = InetAddress.getByName(address);
        this.networkInterface = networkInterface;
        bindAddr = new InetSocketAddress(port);

        toCconnect = group != null && networkInterface != null;
    }

    public InputStream connect(String name) throws IOException {
        if (toCconnect) {
            socket = DatagramChannel.open(StandardProtocolFamily.INET);
            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socket.bind(bindAddr);
            socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
            key = socket.join(group, networkInterface);

            // this will configure the socket to be non blocking
            socket.configureBlocking(false);

            //String feedId = group.getHostName();
            log.info(name + " multicast group: " + group + ", local ip: " + networkInterface.getDisplayName());
        }

        return null; // no usage of InputStream in non blocking NIO sockets
    }


    public SocketAddress receive() {
        buffer.clear();
        try {
            SocketAddress socketAddress = socket.receive(buffer);
            buffer.flip();
            return socketAddress;
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }
    public boolean canConnect() {
        return toCconnect;
    }
    public boolean isClosed() {
        return !socket.isOpen();
    }
    public void setDatagramPacket(DatagramPacket datagramPacket) {
        // not relevant for non blocking socket
    }
    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    public ByteBuffer getBuffer() {
        return buffer;
    }
    public DatagramPacket getPacket() {
        return null;
    }
    public DatagramChannel getSocket() {
        return socket;
    }
    public boolean isNonBlocking() {
        return true;
    }
    public void close() {
        try {
            if (key != null) {
                key.drop();
                log.info(" drop " + key + " " + group + " " + socket);
            }
            if (socket != null)
                socket.close();
        } catch (IOException ignored) {
        }
    }

    public String toString() {
        return group.toString();
    }
}
