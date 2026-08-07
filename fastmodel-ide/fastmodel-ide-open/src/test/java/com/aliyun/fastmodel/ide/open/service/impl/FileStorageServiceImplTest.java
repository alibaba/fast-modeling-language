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

package com.aliyun.fastmodel.ide.open.service.impl;

import java.nio.charset.StandardCharsets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.core.io.Resource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.util.StreamUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Security tests for {@link FileStorageServiceImpl}. Uploaded file names must never escape the
 * storage root directory (path traversal protection).
 */
public class FileStorageServiceImplTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FileStorageServiceImpl storage;

    @Before
    public void setUp() throws Exception {
        storage = new FileStorageServiceImpl(temporaryFolder.newFolder("uploads").toPath());
        storage.init();
    }

    private MockMultipartFile file(String originalFilename) {
        return new MockMultipartFile("file", originalFilename, "text/plain",
            "CREATE TABLE t (id INT);".getBytes(StandardCharsets.UTF_8));
    }

    private void assertRejected(String originalFilename) {
        try {
            storage.save(file(originalFilename));
            fail("expected the file name to be rejected: " + originalFilename);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void saveStoresPlainFileInsideRoot() throws Exception {
        Resource resource = storage.save(file("simple.sql"));
        assertTrue(resource.exists());
        String content = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        assertEquals("CREATE TABLE t (id INT);", content);
    }

    @Test
    public void saveRejectsParentTraversal() {
        assertRejected("../../../tmp/evil.sql");
    }

    @Test
    public void saveRejectsDeepTraversal() {
        assertRejected("../../../../../../etc/cron.d/evil");
    }

    @Test
    public void saveRejectsBackslashTraversal() {
        assertRejected("..\\..\\evil.sql");
    }

    @Test
    public void saveRejectsAbsolutePath() {
        assertRejected("/etc/passwd");
    }

    @Test
    public void saveRejectsBlankFileName() {
        assertRejected("   ");
    }

    @Test
    public void saveRejectsDotComponents() {
        assertRejected("..");
        assertRejected(".");
    }

    @Test
    public void saveRejectsControlCharacters() {
        assertRejected("evil\u0000.sql");
        assertRejected("evil\n.sql");
    }

    @Test
    public void saveAllowsConsecutiveDotsInsidePlainName() throws Exception {
        // separators are banned, so dots inside a plain name cannot traverse
        Resource resource = storage.save(file("report..final.sql"));
        assertTrue(resource.exists());
        assertEquals("report..final.sql", resource.getFilename());
    }

    @Test
    public void loadRejectsTraversal() {
        try {
            storage.load("../../etc/passwd");
            fail("expected the load path to be rejected");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void saveThenLoadRoundTrip() throws Exception {
        storage.save(file("round_trip.sql"));
        Resource loaded = storage.load("round_trip.sql");
        assertTrue(loaded.exists());
        String content = StreamUtils.copyToString(loaded.getInputStream(), StandardCharsets.UTF_8);
        assertEquals("CREATE TABLE t (id INT);", content);
    }

    @Test
    public void deleteAllKeepsStorageUsable() throws Exception {
        storage.save(file("to_delete.sql"));
        storage.deleteAll();
        // save must keep working after deleteAll removed the root directory
        Resource resource = storage.save(file("after_delete.sql"));
        assertTrue(resource.exists());
    }
}
