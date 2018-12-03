package eu.europeana.cloud.dps.topologies.media.support;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.metis.mediaprocessing.exception.MediaException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempFileSync {

    private static final Logger logger = LoggerFactory.getLogger(TempFileSync.class);

    private static final String GET = "get ";
    private static final String DEL = "del ";
    private static final String[] COMMANDS = { GET, DEL };

    private static volatile int startedCount = 0;
    private static InetAddress localAddress;
    private static InetAddress publicAddress;
    private static int port;
    private static ServerSocket serverSocket;
    private static ExecutorService threadPool;
    private static File tempDir;
    private static ConcurrentHashMap<File, File> localToRemoteContent = new ConcurrentHashMap<>();

    private TempFileSync() {
        // hide constructor
    }

    public static synchronized void init(Map<String, Object> config) {
        if (tempDir != null)
            return;
        localAddress = getLocalAddress();
        Map<?, ?> hostMapping = (Map<?, ?>) config.get("MEDIATOPOLOGY_FILE_TRANSFER_HOSTS");
        String host = hostMapping != null ? (String) hostMapping.get(localAddress.getHostAddress()) : null;
        try {
            publicAddress = host == null ? localAddress : InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Invalid public host configuration for " + localAddress, e);
        }
        port = (int) (long) config.get("MEDIATOPOLOGY_FILE_TRANSFER_PORT");
        tempDir = new File(System.getProperty("java.io.tmpdir"));
    }

    public static synchronized InetAddress startServer(Map<String, Object> config) {
        startedCount++;
        if (startedCount == 1) {
            init(config);
            try {
                try {
                    serverSocket = new ServerSocket(port, 100, localAddress);
                } catch (BindException e) {
                    if (testLocalInstance()) {
                        logger.warn("File transfer started by another process");
                        return publicAddress;
                    } else {
                        throw e;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("could not start listening on " + localAddress + ":" + port, e);
            }
            threadPool = Executors.newCachedThreadPool();
            new Thread(TempFileSync::listenerLoop, "file-transfer-listener-loop").start();
            logger.info("file transfer listening on {}:{} (public: {})", localAddress, port, publicAddress);
        }
        return publicAddress;
    }

    public static synchronized void stopServer() {
        startedCount--;
        if (startedCount == 0 && threadPool != null) {
            threadPool.shutdown();
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("could not close server socket", e);
            }
            logger.info("file transfer stopped");
        }
    }

    public static void ensureLocal(FileInfo file) throws MediaException {
        if (isLocal(file) || localToRemoteContent.contains(file.getContent()))
            return;
        long start = System.currentTimeMillis();
        File remote = file.getContent();
        try {
            File local = File.createTempFile("media-sync", null);
            localToRemoteContent.put(local, remote);
            file.setContent(local);

            try (Socket socket = new Socket(file.getContentSource(), port)) {
                PrintStream out = new PrintStream(socket.getOutputStream(), true);
                out.println(GET + remote.getAbsolutePath());
                try (FileOutputStream fileStream = new FileOutputStream(local)) {
                    IOUtils.copy(socket.getInputStream(), fileStream);
                }
            }
            logger.debug("Synching file ({} B) took {} ms", local.length(), System.currentTimeMillis() - start);
        } catch (IOException e) {
            throw new MediaException("Could not synchronize temp file with " + file.getContentSource()
                    + "(" + remote + ")", "TEMP-SYNCH", e);
        }
    }

    public static void delete(FileInfo file) {
        if (file.getContent() != null && !file.getContent().delete())
            logger.warn("could not delete temp file: {}", file.getContent());
        if (isLocal(file))
            return;
        File remote = localToRemoteContent.remove(file.getContent());
        try (Socket socket = new Socket(file.getContentSource(), port)) {
            PrintStream out = new PrintStream(socket.getOutputStream());
            out.println(DEL + remote.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Error sending delete request to " + file.getContentSource() + "(" + remote + ")", e);
        }
    }

    private static InetAddress getLocalAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (!address.isLoopbackAddress() && address instanceof Inet4Address)
                        return address;
                }
            }
            throw new RuntimeException("Could not find network interface to listen at");
        } catch (SocketException e) {
            throw new RuntimeException("Could not find network interface to listen at", e);
        }
    }

    private static boolean isLocal(FileInfo file) {
        return localAddress.equals(file.getContentSource()) || publicAddress.equals(file.getContentSource())
                || file.getContent() == null;
    }

    private static boolean testLocalInstance() throws IOException {
        byte[] content = "230823048103".getBytes("UTF-8");
        File tempFile = File.createTempFile("test", ".tmp");
        try {
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.write(content);
            }
            try (Socket socket = new Socket(localAddress, port)) {
                PrintStream out = new PrintStream(socket.getOutputStream(), true);
                out.println(GET + tempFile.getAbsolutePath());
                byte[] response = IOUtils.toByteArray(socket.getInputStream());
                return Arrays.equals(content, response);
            }
        } finally {
            Files.delete(tempFile.toPath());
        }
    }

    private static void listenerLoop() {
        try {
            while (startedCount > 0) {
                @SuppressWarnings("resource")
                Socket socket = serverSocket.accept();
                threadPool.execute(() -> handleRequest(socket));
            }
        } catch (SocketException e) {
            if (startedCount != 0)
                logger.error("network problem", e);
        } catch (IOException e) {
            logger.error("network problem", e);
        }
    }

    private static void handleRequest(Socket socket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line = reader.readLine();
            String command = null;
            File file = null;
            for (String c : COMMANDS) {
                if (line.startsWith(c)) {
                    command = c;
                    file = new File(line.substring(c.length()));
                }
            }
            if (file == null || !file.isFile() || !file.getAbsolutePath().startsWith(tempDir.getAbsolutePath())) {
                logger.warn("Ignoring invalid request: {}", line);
                return;
            }
            if (command == GET) {
                try (FileInputStream fileStream = new FileInputStream(file)) {
                    IOUtils.copyLarge(fileStream, socket.getOutputStream(), new byte[256 * 1024]);
                }
            } else if (command == DEL) {
                if (!file.delete())
                    logger.warn("Could not delete temp file: {}", file);
            }
        } catch (IOException e) {
            logger.error("network problem", e);
        }
    }
}
