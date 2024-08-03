package edu.colorado.cires.wod.spark.w2p;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(
    name = "http-download",
    description = "HTTP file download operations",
    mixinStandardHelpOptions = true)
public class OsPoolHttpDownloader implements Runnable {

  @Option(names = {"-url", "--url"}, required = true, description = "The URL to download")
  private String url;
  @Option(names = {"-o", "--output-file"}, required = true, description = "The file to download to")
  private Path file;

  @Override
  public void run() {
    HttpClientBuilder clientBuilder;
    try {
      clientBuilder = HttpClients.custom();
      final SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build();
      final ConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
      final Registry<ConnectionSocketFactory> socketFactoryRegistry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https", sslsf)
              .register("http", new PlainConnectionSocketFactory())
              .build();
      final BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager(socketFactoryRegistry);
      connectionManager.setConnectionConfig(ConnectionConfig.custom()
          .setSocketTimeout(Timeout.ofMinutes(1))
          .setConnectTimeout(Timeout.ofMinutes(1))
          .setTimeToLive(TimeValue.ofMinutes(10))
          .build());
      connectionManager.setSocketConfig(SocketConfig.custom().setSoTimeout(Timeout.ofMinutes(1)).build());
      clientBuilder.setConnectionManager(connectionManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try (final CloseableHttpClient httpclient = clientBuilder.build()) {
      final HttpGet httpget = new HttpGet(url);
      final Integer result = httpclient.execute(httpget, response -> {
        try {
          if (response.getCode() >= 200 && response.getCode() < 300) {
            Path parent = file.getParent();
            if (parent != null) {
              Files.createDirectories(parent);
            }
            try (
                InputStream in = response.getEntity().getContent();
                OutputStream out = Files.newOutputStream(file);
            ) {
              IOUtils.copy(in, out);
            }
          } else {
            EntityUtils.consume(response.getEntity());
          }
        } catch (Exception e) {
          EntityUtils.consume(response.getEntity());
          throw new RuntimeException(e);
        }
        return response.getCode();
      });
      System.out.println(result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
