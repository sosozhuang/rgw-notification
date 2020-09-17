package io.ceph.rgw.notification.elasticsearch;

import io.ceph.rgw.client.config.Configuration;
import io.ceph.rgw.client.util.AbstractClosable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides convenience operations of ElasticSearch client.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/5/19.
 */
public class ESClient extends AbstractClosable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ESClient.class);
    private final String index;
    private final RestHighLevelClient client;

    public ESClient(Configuration config) {
        ESProperties properties = new ESProperties(config);
        this.index = Validate.notBlank(properties.getIndex(), "index cannot be empty string");
        HttpHost[] hosts = properties.getHosts().stream().map(h -> {
            Map.Entry<String, Integer> e = parseHost(h);
            return new HttpHost(e.getKey(), e.getValue(), properties.getScheme());
        }).toArray(HttpHost[]::new);
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts cannot be empty");
        }
        RestClientBuilder builder = RestClient.builder(hosts);
        builder.setHttpClientConfigCallback(b -> {
            b.setMaxConnTotal(properties.getMaxConnections());
            if (StringUtils.isNotBlank(properties.getUsername()) && StringUtils.isNotBlank(properties.getPassword())) {
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(properties.getUsername(), properties.getPassword()));
                b.setDefaultCredentialsProvider(credentialsProvider);
            }
            if ("https".equalsIgnoreCase(properties.getScheme())) {
                try {
                    b.setSSLContext(buildSSLContext(properties.getKeyPath(), properties.getCertPath(), properties.getCaPath(), properties.getKeyPass()));
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
            }
            return b;
        }).setRequestConfigCallback(b -> {
            b.setCircularRedirectsAllowed(false);
            b.setSocketTimeout(properties.getSocketTimeout());
            b.setConnectTimeout(properties.getConnectionTimeout());
            b.setConnectionRequestTimeout(properties.getConnectionRequestTimeout());
            return b;
        });
        this.client = new RestHighLevelClient(builder);
        if (!existsIndex()) {
            LOGGER.info("Index [{}] not exists, going to create.", index);
            createIndex();
        }
    }

    private SSLContext buildSSLContext(String key, String cert, String trustedCerts, String password) throws IOException, CertificateException,
            NoSuchPaddingException, NoSuchAlgorithmException, KeyException, InvalidAlgorithmParameterException, InvalidKeySpecException {
        return createSSLContext(toX509Certificates(Paths.get(trustedCerts).toAbsolutePath().toFile()), null, toX509Certificates(Paths.get(cert).toAbsolutePath().toFile()), toPrivateKey(Paths.get(key).toAbsolutePath().toFile(), password), password, null);
    }

    private static X509Certificate[] toX509Certificates(File file) throws CertificateException {
        if (file == null) {
            return null;
        }
        return getCertificatesFromBuffers(readCertificates(file));
    }

    private static List<byte[]> readCertificates(File file) throws CertificateException {
        try {
            FileInputStream in = new FileInputStream(file);
            List<byte[]> certs;
            try {
                certs = readCertificates(in);
            } finally {
                safeClose(in);
            }
            return certs;
        } catch (FileNotFoundException e) {
            throw new CertificateException("cannot find cert file: " + file);
        }
    }

    private static final Pattern CERT_PATTERN = Pattern.compile("-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*CERTIFICATE[^-]*-+", 2);

    private static List<byte[]> readCertificates(InputStream in) throws CertificateException {
        String content;
        try {
            content = readContent(in);
        } catch (IOException e) {
            throw new CertificateException("failed to read certificate content", e);
        }

        List<byte[]> certs = new ArrayList<>();
        Matcher m = CERT_PATTERN.matcher(content);
        for (int start = 0; m.find(start); start = m.end()) {
            certs.add(Base64.getMimeDecoder().decode(m.group(1)));
        }

        if (certs.isEmpty()) {
            throw new CertificateException("cannot find any certificates");
        }
        return certs;
    }

    private static String readContent(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            byte[] buf = new byte[4196];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
            return out.toString("US-ASCII");
        } finally {
            safeClose(out);
        }
    }

    private static void safeClose(InputStream in) {
        if (in == null) {
            return;
        }
        try {
            in.close();
        } catch (IOException e) {
            LOGGER.warn("Failed to close input stream.", e);
        }
    }

    private static void safeClose(OutputStream out) {
        if (out == null) {
            return;
        }
        try {
            out.close();
        } catch (IOException e) {
            LOGGER.warn("Failed to close output stream.", e);
        }
    }

    private static X509Certificate[] getCertificatesFromBuffers(List<byte[]> certs) throws CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate[] x509Certs = new X509Certificate[certs.size()];
        int i = 0;
        ByteArrayInputStream is = null;
        for (; i < certs.size(); ++i) {
            try {
                is = new ByteArrayInputStream(certs.get(i));
                x509Certs[i] = (X509Certificate) cf.generateCertificate(is);
            } finally {
                safeClose(is);
            }
        }
        return x509Certs;
    }

    private static SSLContext createSSLContext(X509Certificate[] trustCertCollection, TrustManagerFactory trustManagerFactory, X509Certificate[] keyCertChain, PrivateKey key, String keyPassword, KeyManagerFactory keyManagerFactory) throws SSLException {
        try {
            if (trustCertCollection != null) {
                trustManagerFactory = buildTrustManagerFactory(trustCertCollection, trustManagerFactory);
            }

            if (keyCertChain != null) {
                keyManagerFactory = buildKeyManagerFactory(keyCertChain, key, keyPassword, keyManagerFactory);
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
                    trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers(),
                    null);
            context.getClientSessionContext().setSessionCacheSize(0);
            context.getClientSessionContext().setSessionTimeout(0);
            return context;
        } catch (SSLException e) {
            throw e;
        } catch (Exception e) {
            throw new SSLException(e);
        }
    }

    private static TrustManagerFactory buildTrustManagerFactory(X509Certificate[] certs, TrustManagerFactory trustManagerFactory) throws NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        int len = certs.length;
        for (int i = 0; i < len; ) {
            X509Certificate cert = certs[i];
            ks.setCertificateEntry(Integer.toString(++i), cert);
        }
        if (trustManagerFactory == null) {
            trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        }
        trustManagerFactory.init(ks);
        return trustManagerFactory;
    }

    private static KeyManagerFactory buildKeyManagerFactory(X509Certificate[] certChain, PrivateKey key, String keyPassword, KeyManagerFactory kmf) throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
        if (algorithm == null) {
            algorithm = "SunX509";
        }
        return buildKeyManagerFactory(certChain, algorithm, key, keyPassword, kmf);
    }

    private static KeyManagerFactory buildKeyManagerFactory(X509Certificate[] certChainFile, String keyAlgorithm, PrivateKey key, String keyPassword, KeyManagerFactory kmf) throws KeyStoreException, NoSuchAlgorithmException, IOException, CertificateException, UnrecoverableKeyException {
        char[] keyPasswordChars = keyPassword == null || keyPassword.length() == 0 ? new char[]{} : keyPassword.toCharArray();
        KeyStore ks = buildKeyStore(certChainFile, key, keyPasswordChars);
        if (kmf == null) {
            kmf = KeyManagerFactory.getInstance(keyAlgorithm);
        }
        kmf.init(ks, keyPasswordChars);
        return kmf;
    }

    private static KeyStore buildKeyStore(X509Certificate[] certChain, PrivateKey key, char[] keyPasswordChars) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        ks.setKeyEntry("key", key, keyPasswordChars, certChain);
        return ks;
    }

    private static PrivateKey toPrivateKey(File keyFile, String keyPassword) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeySpecException,
            InvalidAlgorithmParameterException,
            KeyException, IOException {
        if (keyFile == null) {
            return null;
        }
        return getPrivateKeyFromBytes(readPrivateKey(keyFile), keyPassword);
    }

    private static byte[] readPrivateKey(File file) throws KeyException {
        try {
            InputStream in = new FileInputStream(file);
            try {
                return readPrivateKey(in);
            } finally {
                safeClose(in);
            }
        } catch (FileNotFoundException e) {
            throw new KeyException("could not fine key file: " + file);
        }
    }

    private static final Pattern KEY_PATTERN = Pattern.compile("-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*PRIVATE\\s+KEY[^-]*-+", Pattern.CASE_INSENSITIVE);

    private static byte[] readPrivateKey(InputStream in) throws KeyException {
        String content;
        try {
            content = readContent(in);
        } catch (IOException e) {
            throw new KeyException("failed to read key input stream", e);
        }

        Matcher m = KEY_PATTERN.matcher(content);
        if (!m.find()) {
            throw new KeyException("cannot find a PKCS #8 private key in input stream");
        }
        return Base64.getMimeDecoder().decode(m.group(1));
    }

    private static PrivateKey getPrivateKeyFromBytes(byte[] encodedKey, String keyPassword)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException,
            InvalidAlgorithmParameterException, KeyException, IOException {
        PKCS8EncodedKeySpec encodedKeySpec = generateKeySpec(
                keyPassword == null ? null : keyPassword.toCharArray(), encodedKey);
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(encodedKeySpec);
        } catch (InvalidKeySpecException ignore) {
            try {
                return KeyFactory.getInstance("DSA").generatePrivate(encodedKeySpec);
            } catch (InvalidKeySpecException ignore2) {
                try {
                    return KeyFactory.getInstance("EC").generatePrivate(encodedKeySpec);
                } catch (InvalidKeySpecException e) {
                    throw new InvalidKeySpecException("Neither RSA, DSA nor EC worked", e);
                }
            }
        }
    }

    private static PKCS8EncodedKeySpec generateKeySpec(char[] password, byte[] key)
            throws IOException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException,
            InvalidKeyException, InvalidAlgorithmParameterException {

        if (password == null) {
            return new PKCS8EncodedKeySpec(key);
        }

        EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(encryptedPrivateKeyInfo.getAlgName());
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
        SecretKey pbeKey = keyFactory.generateSecret(pbeKeySpec);

        Cipher cipher = Cipher.getInstance(encryptedPrivateKeyInfo.getAlgName());
        cipher.init(Cipher.DECRYPT_MODE, pbeKey, encryptedPrivateKeyInfo.getAlgParameters());

        return encryptedPrivateKeyInfo.getKeySpec(cipher);
    }

    private static Map.Entry<String, Integer> parseHost(String host) {
        String[] result = host.split(":");
        if (result.length != 2) {
            throw new IllegalArgumentException("cannot parse host: " + host);
        }
        return new AbstractMap.SimpleImmutableEntry<>(result[0], Integer.parseInt(result[1]));
    }

    private boolean existsIndex() {
        GetIndexRequest request = new GetIndexRequest(index);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private boolean createIndex() {
        try {
            CreateIndexRequest request = new CreateIndexRequest(index);
            XContentBuilder source = XContentFactory.jsonBuilder();
            source.startObject().startArray("dynamic_templates");

            source.startObject().startObject("bucket");
            source.field("match", "bucket").startObject("mapping");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject().endObject().endObject();

            source.startObject().startObject("name");
            source.field("match", "name").startObject("mapping");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject().endObject().endObject();

            source.startObject().startObject("instance");
            source.field("match", "instance").startObject("mapping");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject().endObject().endObject();

            source.startObject().startObject("create_time");
            source.field("match", "create_time").startObject("mapping");
            source.field("type", "date").field("index", true).field("format", "yyyy-MM-dd HH:mm:ss.SSS");
            source.endObject().endObject().endObject();

            source.startObject().startObject("meta");
            source.field("match", "meta").startObject("mapping");
            source.field("type", "nested").startObject("properties");

            source.startObject("cache_control");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("content_disposition");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("content_encoding");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("content_language");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("content_length");
            source.field("type", "long").field("index", false);
            source.endObject();

            source.startObject("md5");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("content_type");
            source.field("type", "text").field("index", true);
            source.startObject("fields");
            source.startObject("keyword").field("type", "keyword").endObject();
            source.endObject().endObject();

            source.startObject("expires_date");
            source.field("type", "date").field("index", true).field("format", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'");
            source.endObject();

            source.startObject("user");
            source.field("type", "nested").startObject("properties");

            source.endObject().endObject();
            source.endObject().endObject().endObject().endObject();

            source.startObject().startObject("user_meta").field("path_match", "meta.user.*").startObject("mapping")
                    .field("type", "text").field("index", true)
                    .endObject().endObject().endObject();

            source.startObject().startObject("disable").field("match", "*").startObject("mapping")
                    .field("type", "x").field("enable", Boolean.FALSE)
                    .endObject().endObject().endObject();
            source.endArray().endObject();
            request.mapping(source);
            request.settings(Settings.builder().put("index.number_of_shards", 5));
            return client.indices().create(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("resource_already_exists")) {
                LOGGER.warn("Index [{}] already exists.", index, e);
            } else {
                throw e;
            }
        }
        return false;
    }

    public void insert(String id, byte[] source) {
        IndexRequest request = new IndexRequest(index);
        request.source(source, XContentType.JSON)
                .type("_doc")
                .id(id)
                .opType(DocWriteRequest.OpType.INDEX);
        client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                LOGGER.debug("Insert response: [{}].", response);
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Failed to execute insert [{}].", request, e);
            }
        });
    }

    public void delete(String bucket, String key) {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.indices(index);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.termQuery("bucket", bucket)).must(QueryBuilders.termQuery("name", key));
        request.setQuery(queryBuilder).setSize(1);
        client.deleteByQueryAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse response) {
                LOGGER.debug("Delete response: [{}].", response);
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Failed to execute delete [{}].", request, e);
            }
        });
    }

    @Override
    protected void doClose() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close es client.", e);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
