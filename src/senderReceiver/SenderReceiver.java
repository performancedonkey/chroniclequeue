package senderReceiver;

import utils.NanoClock;
import utils.NonBlockingSocket;
import utils.SelectNetDriver;
import org.apache.log4j.Logger;
import setaffinitymanager.SetAffinityManager;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: mati
 * Date: 07/10/2020
 */

public class SenderReceiver {
    private static final Logger log = Logger.getLogger(SenderReceiver.class);

    private static final int resetEvery = 1000000;

    private static ByteBuffer buffer;
    private static int packetLength;

    public static void main(String[] args) {
        try {
            log.info("Welcome to SenderReceiver version 1.1.3");
            System.setProperty("log4j.configurationFile", "log4j2.xml");

            log.info("CurrTimeMillis: " + System.currentTimeMillis());
            log.info("CurrTimeMicros: " + SetAffinityManager.getInstance().getCCurrentMicros());
            log.info("CurrTimeNanos : " + NanoClock.getNanoTimeNow());

            if (args.length < 5) {
                log.fatal("There must be at least 5 args: <-s/-r> <IP> <Port> <InterfaceName> [SleepMillis] "); // [TTLSeconds]
                return;
            }
            int sleep = Integer.parseInt(args[4]);

            java.security.Security.setProperty("networkaddress.cache.ttl", "30");

            String ip = args[1];
            int port = Integer.parseInt(args[2]);
            String interfaceName = args[3];

            if (!args[0].equals("-r") && !args[0].equals("-s")) {
                log.error("Error in first argument! Must be -s for the sender or -r for the receiver");
                return;
            }

            packetLength = args.length > 5 ? Integer.parseInt(args[5]) : 12; // long + int
            buffer = ByteBuffer.allocate(packetLength);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            boolean isSender = args[0].equals("-s");
            if (isSender)
                new SenderReceiver().doSenderCode(ip, port, interfaceName, sleep);
            else new SenderReceiver().doReceiverCode(ip, port, interfaceName);

        } catch (Exception e) {
            log.error("Fatal error occurred!", e);
        } finally {
            log.fatal("Stopped");
        }
    }

    int packetSeq = 0;

    private void doSenderCode(String ip, int port, String interfaceName, int sleep) {
        log.info("Doing sender code on: " + ip + " / " + port + " / " + interfaceName + " / " + sleep);
        try (MulticastSocket senderSocket = new MulticastSocket()) {
            InetAddress group = InetAddress.getByName(ip);
            byte[] buf = buffer.array();
            DatagramPacket datagram = new DatagramPacket(buf, buf.length, group, port);

            senderSocket.setTimeToLive(30);
            InetAddress address = InetAddress.getByName(interfaceName);
            NetworkInterface nif = NetworkInterface.getByInetAddress(address);
            senderSocket.setNetworkInterface(nif);

            while (running) {
                long sendingTime = sendPacket(senderSocket, datagram);

                log.info(String.format("Sending packet %d @ %d", packetSeq, sendingTime));

                Thread.sleep(sleep);
            }
        } catch (Exception e) {
            System.out.println("Error in Sender code: " + e);
        } finally {
            System.out.println("Stopped sending");
        }
    }

    private long sendPacket(MulticastSocket senderSocket, DatagramPacket datagram) throws IOException {
        long sendingTime = NanoClock.getNanoTimeNow();

        buffer.clear();
        buffer.putInt(0, ++packetSeq);
        buffer.putLong(4, sendingTime);

        datagram.setData(buffer.array(), 0, packetLength);

        senderSocket.send(datagram);

        packetSeq %= resetEvery;

        return sendingTime;
    }

    boolean running = true;
    long lowestRead = Long.MAX_VALUE;
    long lowest = Long.MAX_VALUE;
    int expectedPacketSeq = 0;
    HashSet<Integer> missedPacketSeqs = new HashSet<>();

    private void doReceiverCode(String ip, int port, String interfaceName) {
        log.info("Doing receiver code on: " + ip + " / " + port + " / " + interfaceName);

        NonBlockingSocket receiverSocket = null;
        try {
            // Blocking
//        InetSocketAddress groupRead = new InetSocketAddress(ip, port);
//        DatagramPacket packet = new DatagramPacket(buffer.array(), buffer.capacity());
//
//        MulticastSocket receiverSocket = new MulticastSocket(port);
//        receiverSocket.setReceiveBufferSize(buffer.capacity());
//        receiverSocket.joinGroup(groupRead, NetworkInterface.getByName(interfaceName));

            // Non Blocking
            NetworkInterface nic = SelectNetDriver.getInst(interfaceName).getNetworkInterface();
            receiverSocket = new NonBlockingSocket(ip, port, nic);
            receiverSocket.connect(interfaceName);
            receiverSocket.setBuffer(buffer);
        } catch (Exception e) {
            log.error("Error in Receiver startup code!", e);
//        } finally {
            //            receiverSocket.leaveGroup(groupRead.getAddress());
        }

        while (running && receiverSocket != null) {

            // Blocking
//                buffer.clear();
//                receiverSocket.receive(packet);
//                buffer.flip();
//                buffer.limit(packet.getLength());

            long readTime = NanoClock.getNanoTimeNow();
            // Non-blocking
            receiverSocket.receive();
            // Sample time as quickly as possible
            long receiveTime = NanoClock.getNanoTimeNow();
            // If empty, restart loop
            if (!buffer.hasRemaining()) continue;

            lowestRead = Math.min(lowestRead, receiveTime - readTime);

            if (!checkSequence(buffer))
                log.error("PacketLoss");

            long sendingTime = readPayload(buffer);

            long diff = receiveTime - sendingTime;
            if (lowest > diff) {
                lowest = diff;
                log.warn(String.format("New lowest latency found (ns) %d \t|Lowest read (ns) %d", lowest, lowestRead));
            }

            if (expectedPacketSeq % 500 == 0)
                log.info(String.format("Received packet %d @ %d \t| ltcy (us): %.3f \t| Above lowest (ns): %d",
                        expectedPacketSeq, receiveTime, diff / 1000f, diff - lowest));
        }

        if (receiverSocket != null) {
            receiverSocket.close();
        }
        log.info("Stopped receiving");
    }

    private boolean checkSequence(ByteBuffer buffer) {
        int packetSeq = buffer.getInt(0);
        if (packetSeq == expectedPacketSeq) {
            expectedPacketSeq = (expectedPacketSeq + 1) % resetEvery;
            return true;
        } else {
            if (expectedPacketSeq > packetSeq) {
                if (!missedPacketSeqs.contains(packetSeq))
                    log.error("Something went wrong. No missed seq in missedPacketSeqs. PacketSeq: " + packetSeq);
                else
                    missedPacketSeqs.remove(packetSeq);
            }
            log.warn("Expected packetSeq: " + expectedPacketSeq + " while actual was: " + packetSeq);
            if (packetSeq > expectedPacketSeq) {
                missedPacketSeqs.add(expectedPacketSeq);
                expectedPacketSeq = packetSeq + 1;
            }
            return false;
        }
    }

    private long readPayload(ByteBuffer buffer) {
        return buffer.getLong(4);
    }
}
