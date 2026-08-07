/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.ide.spi.receiver.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Locale;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.ide.spi.exception.PlatformException;
import com.aliyun.fastmodel.ide.spi.exception.error.PlatformErrorCode;
import com.aliyun.fastmodel.ide.spi.receiver.ToolService;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext.ReverseTargetStrategy;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 默认的工具服务
 *
 * @author panguanjing
 * @date 2022/1/12
 */
public class DefaultToolServiceImpl implements ToolService {

    /**
     * Maximum number of bytes allowed when importing SQL by URI, to prevent a single request from
     * fetching unbounded content.
     */
    private static final long MAX_IMPORT_BYTES = 10 * 1024 * 1024L;

    /**
     * Connect/read timeouts for the outbound import fetch. Without them a hostile or broken peer
     * could pin a request thread forever on the unauthenticated import endpoint.
     */
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;

    private static final int READ_TIMEOUT_MILLIS = 30_000;

    private final TransformerFactory transformerFactory;

    private final FastModelParser fastModelParser;

    public DefaultToolServiceImpl() {
        this.transformerFactory = TransformerFactory.getInstance();
        this.fastModelParser = FastModelParserFactory.getInstance().get();
    }

    @Override
    public String importSql(String text, DialectMeta dialectMeta) {
        Transformer<Node> transformer = transformerFactory.get(dialectMeta);
        DialectNode dialectNode = new DialectNode(text);
        return transformer.reverse(dialectNode,
            ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build()).toString();
    }

    @Override
    public String exportSql(String fml, DialectMeta dialectMeta) {
        Transformer<Node> transformer = transformerFactory.get(dialectMeta);
        Node node = fastModelParser.parseStatement(fml);
        DialectNode dialectNode = transformer.transform(node);
        return dialectNode.getNode();
    }

    @Override
    public String importByUri(String uri, DialectMeta dialectMeta) {
        URI url;
        try {
            url = new URI(uri);
        } catch (URISyntaxException e) {
            throw new PlatformException("import by uri error" + uri, PlatformErrorCode.URL_INVALID_ERROR, e);
        }
        InetAddress[] validatedAddresses = validateUri(url);
        String text;
        try {
            text = fetchContent(url, validatedAddresses[0]);
        } catch (IOException e) {
            throw new PlatformException("import by uri error" + uri, PlatformErrorCode.READ_FILE_ERROR, e);
        }
        return importSql(text, dialectMeta);
    }

    /**
     * Fetches the content with the size bound applied. Package-private so tests can exercise the
     * bound without touching the network.
     */
    String fetchContent(URI uri, InetAddress validatedAddress) throws IOException {
        try (InputStream in = new BoundedInputStream(openStream(uri, validatedAddress), MAX_IMPORT_BYTES + 1)) {
            byte[] bytes = IOUtils.toByteArray(in);
            if (bytes.length > MAX_IMPORT_BYTES) {
                throw new PlatformException("import by uri error, content exceeds " + MAX_IMPORT_BYTES + " bytes: "
                    + uri, PlatformErrorCode.URL_INVALID_ERROR);
            }
            return new String(bytes, UTF_8);
        }
    }

    /**
     * Only allow fetching from http/https network addresses, and prohibit reading local files
     * (such as file://) or accessing internal network addresses (loopback, private networks,
     * RFC 6598 shared space which hosts the cloud metadata endpoint, link-local and IPv6
     * unique-local addresses), to prevent arbitrary file reads and SSRF.
     *
     * @return the resolved addresses, all validated; the caller must connect to one of these so
     * the validated resolution is the one actually used.
     */
    private InetAddress[] validateUri(URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.toLowerCase(Locale.ROOT).equals("http")
            || scheme.toLowerCase(Locale.ROOT).equals("https"))) {
            throw new PlatformException("import by uri error, unsupported scheme: " + scheme
                + ", only http/https are allowed", PlatformErrorCode.URL_INVALID_ERROR);
        }
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            throw new PlatformException("import by uri error, missing host: " + uri,
                PlatformErrorCode.URL_INVALID_ERROR);
        }
        if (uri.getUserInfo() != null) {
            throw new PlatformException("import by uri error, userinfo in uri is not allowed: " + uri,
                PlatformErrorCode.URL_INVALID_ERROR);
        }
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new PlatformException("import by uri error, unknown host: " + host,
                PlatformErrorCode.URL_INVALID_ERROR, e);
        }
        for (InetAddress address : addresses) {
            if (isBlockedAddress(address)) {
                throw new PlatformException("import by uri error, access to internal network address is not allowed: "
                    + host, PlatformErrorCode.URL_INVALID_ERROR);
            }
        }
        return addresses;
    }

    /**
     * {@link InetAddress} predicates do not cover RFC 6598 shared address space (100.64.0.0/10,
     * which contains the ECS metadata endpoint) or IPv6 unique-local addresses (fc00::/7), and
     * {@code isSiteLocalAddress()} only matches the deprecated fec0::/10 for IPv6, so these
     * ranges are checked explicitly.
     */
    private boolean isBlockedAddress(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
            || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return true;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 16 && isIpv4Mapped(bytes)) {
            bytes = new byte[] {bytes[12], bytes[13], bytes[14], bytes[15]};
        }
        if (bytes.length == 4) {
            int first = bytes[0] & 0xff;
            int second = bytes[1] & 0xff;
            // 0.0.0.0/8 ("this" network)
            if (first == 0) {
                return true;
            }
            // 100.64.0.0/10, RFC 6598 shared address space
            return first == 100 && (second & 0xc0) == 64;
        }
        if (bytes.length == 16) {
            // fc00::/7, IPv6 unique-local addresses
            return (bytes[0] & 0xfe) == 0xfc;
        }
        return false;
    }

    private boolean isIpv4Mapped(byte[] bytes) {
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return (bytes[10] & 0xff) == 0xff && (bytes[11] & 0xff) == 0xff;
    }

    /**
     * Opens the import connection. Redirects are never followed: {@code validateUri} only saw the
     * original host, so a 3xx hop could otherwise point the fetch at an internal address. For
     * plain http the connection is pinned to the validated address literal (with the original
     * Host header), closing the DNS re-binding window between validation and connect. For https
     * the host name is kept so certificate validation keeps working; the re-binding window there
     * is narrowed by the disabled redirects and the JVM DNS cache. Package-private so tests can
     * substitute a local stream without touching the network.
     */
    InputStream openStream(URI uri, InetAddress validatedAddress) throws IOException {
        if ("http".equalsIgnoreCase(uri.getScheme())) {
            URL pinned = pinnedUrl(uri, validatedAddress);
            HttpURLConnection conn = (HttpURLConnection)pinned.openConnection();
            conn.setRequestProperty("Host", hostHeader(uri));
            return checkedStream(conn, uri);
        }
        HttpURLConnection conn = (HttpURLConnection)uri.toURL().openConnection();
        return checkedStream(conn, uri);
    }

    private InputStream checkedStream(HttpURLConnection conn, URI uri) throws IOException {
        conn.setInstanceFollowRedirects(false);
        conn.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        conn.setReadTimeout(READ_TIMEOUT_MILLIS);
        int status = conn.getResponseCode();
        if (status >= 300 && status < 400) {
            conn.disconnect();
            throw new PlatformException("import by uri error, redirects are not allowed: " + uri,
                PlatformErrorCode.URL_INVALID_ERROR);
        }
        if (status >= 400) {
            conn.disconnect();
            throw new IOException("import by uri error, unexpected HTTP status " + status + ": " + uri);
        }
        return conn.getInputStream();
    }

    private URL pinnedUrl(URI uri, InetAddress address) throws MalformedURLException {
        String hostLiteral = address.getHostAddress();
        if (address instanceof Inet6Address) {
            int scope = hostLiteral.indexOf('%');
            if (scope >= 0) {
                hostLiteral = hostLiteral.substring(0, scope);
            }
            hostLiteral = "[" + hostLiteral + "]";
        }
        String file = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        if (uri.getRawQuery() != null) {
            file = file + "?" + uri.getRawQuery();
        }
        return new URL("http", hostLiteral, uri.getPort(), file);
    }

    private String hostHeader(URI uri) {
        String host = uri.getHost();
        return uri.getPort() > 0 ? host + ":" + uri.getPort() : host;
    }
}
