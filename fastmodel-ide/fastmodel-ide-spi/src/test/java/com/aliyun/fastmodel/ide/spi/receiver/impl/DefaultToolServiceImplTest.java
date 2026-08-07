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

import com.aliyun.fastmodel.ide.spi.exception.PlatformException;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Security tests for {@link DefaultToolServiceImpl#importByUri(String, DialectMeta)}.
 * The URI import must only allow external http/https resources and must never read local
 * files or internal network addresses.
 */
public class DefaultToolServiceImplTest {

    private final DefaultToolServiceImpl toolService = new DefaultToolServiceImpl();

    private final DialectMeta dialectMeta = DialectMeta.getByName(DialectName.MYSQL);

    private void assertRejected(String uri, String expectedMessagePart) {
        try {
            toolService.importByUri(uri, dialectMeta);
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
    public void importByUriRejectsLoopbackAddress() {
        assertRejected("http://127.0.0.1/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsLinkLocalAddress() {
        // link-local range covers the cloud metadata endpoint
        assertRejected("http://169.254.169.254/latest/meta-data/", "internal network address");
    }

    @Test
    public void importByUriRejectsSiteLocalAddress() {
        assertRejected("http://10.0.0.1/a.sql", "internal network address");
    }

    @Test
    public void importByUriRejectsInvalidSyntax() {
        assertRejected("http://host with space/", "import by uri error");
    }
}
