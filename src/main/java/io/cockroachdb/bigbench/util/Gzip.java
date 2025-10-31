package io.cockroachdb.bigbench.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public abstract class Gzip {
    private Gzip() {
    }

    public static void copy(InputStream in, OutputStream out) throws IOException {
        try (InputStream is = new BufferedInputStream(in)) {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out, true);
            byte[] buffer = new byte[1024 * 8];
            int len;
            while ((len = is.read(buffer)) != -1) {
                gzipOutputStream.write(buffer, 0, len);
            }
            gzipOutputStream.flush();
            gzipOutputStream.finish();
        }
        out.flush();
    }
}
