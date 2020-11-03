package utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class SelectNetDriver {
    private static final Logger log = Logger.getLogger(SelectNetDriver.class);
    private static Map<String, SelectNetDriver> inst = new HashMap<String, SelectNetDriver>();

    private NetworkInterface networkInterface;
    private InetAddress selectedInetAddress;

    public static SelectNetDriver getInst(String ipPrefix) {
        SelectNetDriver netDriver = inst.get(ipPrefix);
        if (netDriver == null) {
            netDriver = new SelectNetDriver(ipPrefix);
            inst.put(ipPrefix, netDriver);
        }
        return netDriver;
    }

    private SelectNetDriver(String ipPrefix) {
        init(ipPrefix);
    }


    private void init(String ipPrefix) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements() && selectedInetAddress == null) {
                networkInterface = interfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (inetAddress.getHostAddress().equals(ipPrefix)) {
                        selectedInetAddress = inetAddress;
                        break;
                    }
                }
            }
            if (selectedInetAddress == null) {
                throw new RuntimeException("Cannot find network interface with ip that starts with - " + ipPrefix);
            }

        } catch (IOException e) {
            log.error("problem with reading data from socket", e);
        }
    }

    public InetAddress getSelectedInetAddress() { return selectedInetAddress; }

    public NetworkInterface getNetworkInterface() { return networkInterface; }
}
