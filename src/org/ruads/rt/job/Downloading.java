
package org.ruads.rt.job;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.nio.ByteBuffer.allocateDirect;

public final class Downloading extends Job {
    private static final int DOWNLOAD_BUFFER_CAPACITY = 500 * 1024;
    private static final int CONNECT_TIMEOUT = 5_000;
    private static final int READ_TIMEOUT = 5_000;
    private static final int MAX_FILE_SIZE = 10 * 1024 * 1024;
    private final byte[] buffer;
    private final ByteBuffer fileContent = allocateDirect(MAX_FILE_SIZE);
    private final HttpURLConnection connection;
    private final String url;
    public Downloading(int sla, int priority, String url) throws IOException {
        super(format("downloading of %s", url), sla, priority);
        this.connection = connect(url);
        this.url = url;
        this.buffer = new byte[DOWNLOAD_BUFFER_CAPACITY];
    }

    @Override
    public Job process() throws IOException {
        InputStream stream = connection.getInputStream();
        int count = stream.read(buffer);
        if (count < 1) {
            System.out.println(format(
                    "file downloaded. got %d bytes. priority %d",
                    fileContent.position(),
                    getPriority()
            ));
            stream.close();
            connection.disconnect();
            return null;//new Parsing(buffer);
        }
        fileContent.put(buffer, 0, count);
        System.out.println(format(
                "downloading file %s, priority %d, progress %d",
                url,
                getPriority(),
                fileContent.position()
        ));
        return this;
    }

    private static HttpURLConnection connect(String url) throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setConnectTimeout(CONNECT_TIMEOUT);
        con.setReadTimeout(READ_TIMEOUT);
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "Java10HttpURLConnection");
        int responseCode = con.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("can't download");
        }
        return con;
    }
}