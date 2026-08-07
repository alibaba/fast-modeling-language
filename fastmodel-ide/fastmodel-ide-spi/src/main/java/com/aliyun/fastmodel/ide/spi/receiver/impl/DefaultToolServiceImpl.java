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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
        validateUri(url);
        try (InputStream in = new BoundedInputStream(url.toURL().openStream(), MAX_IMPORT_BYTES + 1)) {
            byte[] bytes = IOUtils.toByteArray(in);
            if (bytes.length > MAX_IMPORT_BYTES) {
                throw new PlatformException("import by uri error, content exceeds " + MAX_IMPORT_BYTES + " bytes: "
                    + uri, PlatformErrorCode.URL_INVALID_ERROR);
            }
            return importSql(new String(bytes, UTF_8), dialectMeta);
        } catch (IOException e) {
            throw new PlatformException("import by uri error" + uri, PlatformErrorCode.READ_FILE_ERROR, e);
        }
    }

    /**
     * Only allow fetching from http/https network addresses, and prohibit reading local files
     * (such as file://) or accessing internal network addresses (loopback, private networks,
     * link-local addresses like cloud metadata, etc.), to prevent arbitrary file reads and SSRF.
     */
    private void validateUri(URI uri) {
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
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new PlatformException("import by uri error, unknown host: " + host,
                PlatformErrorCode.URL_INVALID_ERROR, e);
        }
        for (InetAddress address : addresses) {
            if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
                throw new PlatformException("import by uri error, access to internal network address is not allowed: "
                    + host, PlatformErrorCode.URL_INVALID_ERROR);
            }
        }
    }
}
