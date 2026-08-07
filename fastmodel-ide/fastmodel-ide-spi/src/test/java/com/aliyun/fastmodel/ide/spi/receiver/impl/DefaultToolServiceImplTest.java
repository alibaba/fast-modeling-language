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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;

import com.aliyun.fastmodel.ide.spi.exception.PlatformException;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Security tests for {@link DefaultToolServiceImpl#importByUri(String,
 * com.aliyun.fastmodel.transform.api.dialect.DialectMeta)}. The URI import must only allow
 * external http/https resources and must never read local files or internal network addresses.
 */
public class DefaultToolServiceImplTest {

    private static final int MAX_IMPORT_BYTES = 10 * 1024 * 1024;

    private final DefaultToolServiceImpl toolService = new DefaultToolServiceImpl();

    private void assertRejected(String uri, String expectedMessagePart) {
        try {
            toolService.importByUri(uri, null);
            fail("expected the uri to be rejected: " + uri);
        } catch (PlatformException e) {
            assertTrue("unexpected message: " + e.getMessage(),
                e.getMessage().contains(expectedMessagePart));
        }
    }

    @Test
    public void importByUriRejectsFileScheme() {
        assertRejected("file:///etc/hosts", "unsupported scheme");
    }

    @Test
    public void importByUriRejectsJarScheme() {
        assertRejected("jar:file:///tmp/x.jar!/a.sql", "unsupported scheme");
    }

    @Test
    public void importByUriRejectsMissingScheme() {
        assertRejected("/etc/hosts", "unsupported scheme");
    }

    @Test
    public void importByUriRejectsMissingHost() {
        assertRejected("http:///some/path.sql", "missing host");
    }

    @Test
    public void importByUriRejectsUserInfo() {
        assertRejected("http://user:pass@93.184.216.34/x.sql", "userinfo");
    }

    @Test
    public void importByUriRejectsLoopbackAddress() {
        assertRejected("http://127.0.0.1/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsLinkLocalAddress() {
        // link-local range covers the AWS-style cloud metadata endpoint
        assertRejected("http://169.254.169.254/latest/meta-data/", "internal network address");
    }

    @Test
    public void importByUriRejectsSiteLocalAddress() {
        assertRejected("http://10.0.0.1/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsSharedAddressSpace() {
        // 100.64.0.0/10 (RFC 6598) hosts the ECS metadata endpoint
        assertRejected("http://100.100.100.200/latest/meta-data/", "internal network address");
    }

    @Test
    public void importByUriRejectsThisNetworkAddress() {
        assertRejected("http://0.1.2.3/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsIpv6UniqueLocalAddress() {
        assertRejected("http://[fd00::1]/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsInvalidSyntax() {
        assertRejected("http://host with space/", "import by uri error");
    }

    private DefaultToolServiceImpl serviceWithStubbedStream(byte[] content) {
        return new DefaultToolServiceImpl() {
            @Override
            InputStream openStream(URI uri, InetAddress validatedAddress) {
                return new ByteArrayInputStream(content);
            }
        };
    }

    private String stubbedFetch(byte[] content) throws Exception {
        DefaultToolServiceImpl service = serviceWithStubbedStream(content);
        return service.fetchContent(new URI("http://93.184.216.34/x.sql"),
            InetAddress.getByName("93.184.216.34"));
    }

    @Test
    public void fetchContentReturnsFetchedText() throws Exception {
        assertEquals("CREATE TABLE t (id INT);",
            stubbedFetch("CREATE TABLE t (id INT);".getBytes(UTF_8)));
    }

    @Test
    public void fetchContentAcceptsExactlyMaxBytes() throws Exception {
        byte[] content = new byte[MAX_IMPORT_BYTES];
        Arrays.fill(content, (byte)'a');
        assertEquals(MAX_IMPORT_BYTES, stubbedFetch(content).length());
    }

    @Test
    public void fetchContentRejectsContentExceedingMaxBytes() throws Exception {
        byte[] content = new byte[MAX_IMPORT_BYTES + 1];
        try {
            stubbedFetch(content);
            fail("expected the oversized content to be rejected");
        } catch (PlatformException e) {
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("exceeds"));
        }
    }
}
