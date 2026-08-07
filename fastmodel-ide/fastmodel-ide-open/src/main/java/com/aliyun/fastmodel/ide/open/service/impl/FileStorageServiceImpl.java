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

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.aliyun.fastmodel.ide.open.service.StorageService;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;
import org.springframework.web.multipart.MultipartFile;

/**
 * 文件存储服务
 *
 * @author panguanjing
 * @date 2021/12/22
 */
@Component
public class FileStorageServiceImpl implements StorageService {

    /**
     * Uploads are stored in a dedicated directory under the temp folder instead of the temp
     * folder itself, so a sanitized file name can never overwrite unrelated temp files.
     */
    private final Path root;

    public FileStorageServiceImpl() {
        this(Paths.get(System.getProperty("java.io.tmpdir"), "fastmodel-ide-uploads"));
    }

    FileStorageServiceImpl(Path root) {
        this.root = root;
    }

    @Override
    @PostConstruct
    public void init() {
        try {
            // a pre-planted symlink at the well-known path would redirect every upload outside
            // the storage root, so refuse to use it
            if (Files.isSymbolicLink(root)) {
                throw new IllegalStateException("Upload folder must not be a symbolic link: " + root);
            }
            Files.createDirectories(root);
            try {
                Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX file system
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize folder for upload!", e);
        }
    }

    @Override
    public Resource save(MultipartFile file) {
        String safeName = sanitizeFilename(file.getOriginalFilename());
        try {
            Path resolve = resolveInRoot(safeName);
            Files.copy(file.getInputStream(), resolve, StandardCopyOption.REPLACE_EXISTING);
            Resource resource = new UrlResource(resolve.toUri());
            return resource;
        } catch (Exception e) {
            throw new RuntimeException("Could not store the file. Error: " + safeName, e);
        }
    }

    @Override
    public Resource load(String filename) {
        try {
            Path file = resolveInRoot(sanitizeFilename(filename));
            Resource resource = new UrlResource(file.toUri());

            if (resource.exists() || resource.isReadable()) {
                return resource;
            } else {
                throw new RuntimeException("Could not read the file!");
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Error: " + e.getMessage());
        }
    }

    /**
     * Rejects any client-supplied file name that is not a plain file name: the exact path
     * components "." and "..", any path separator, and control characters. Consecutive dots
     * inside a plain name (e.g. "report..final.sql") are harmless once separators are banned
     * and are allowed.
     */
    private String sanitizeFilename(String filename) {
        if (filename == null || filename.trim().isEmpty()) {
            throw new IllegalArgumentException("File name must not be empty");
        }
        String name = filename.trim();
        if (name.equals(".") || name.equals("..") || name.contains("/") || name.contains("\\")
            || containsControlCharacter(name)) {
            throw new IllegalArgumentException("Illegal file name: " + filename);
        }
        return name;
    }

    private boolean containsControlCharacter(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c < 0x20 || c == 0x7f) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves the sanitized name against the storage root and guarantees the final path stays
     * inside the root directory (defense in depth against path traversal).
     */
    private Path resolveInRoot(String safeName) {
        Path resolve = root.resolve(safeName).normalize();
        if (!resolve.startsWith(root.normalize())) {
            throw new IllegalArgumentException("Path traversal detected: " + safeName);
        }
        return resolve;
    }

    @Override
    public void deleteAll() {
        FileSystemUtils.deleteRecursively(root.toFile());
        try {
            // deleteRecursively removes the root itself; recreate it so save/loadAll keep working
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new RuntimeException("Could not re-create folder for upload!", e);
        }
    }

    @Override
    public Stream<Path> loadAll() {
        try {
            return Files.walk(root, 1).filter(path -> !path.equals(root)).map(root::relativize);
        } catch (IOException e) {
            throw new RuntimeException("Could not load the files!");
        }
    }
}
