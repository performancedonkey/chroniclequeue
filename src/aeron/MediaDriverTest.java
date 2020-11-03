package aeron;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Paths;

import java.io.File;
import java.net.InetSocketAddress;

public class MediaDriverTest {
    private static final Logger LOG = LoggerFactory.getLogger(MediaDriverTest.class);
    static EchoServer server;

    public static void main(String[] args) {
        DOMConfigurator.configure("./log4j.xml");

        File media_dir = new File(Paths.temp_path + "aeron-Mati/");
        media_dir.mkdir();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 40000);
        LOG.info("run" + address + " / " + media_dir);
        try {
            server = EchoServer.create(media_dir.toPath(), address);
            server.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("running");

//        new LowLatencyMediaDriver();
    }
}
