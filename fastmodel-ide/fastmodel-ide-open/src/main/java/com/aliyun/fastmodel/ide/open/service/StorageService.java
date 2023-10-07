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

package com.aliyun.fastmodel.ide.open.service;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

/**
 * Storage service
 *
 * @author panguanjing
 * @date 2021/12/22
 */
public interface StorageService {
    /**
     * init
     */
    public void init();

    /**
     * save file
     *
     * @param file
     * @return {@link Resource}
     */
    public Resource save(MultipartFile file);

    /**
     * load resource from filename
     *
     * @param filename
     * @return {@link Resource}
     */
    public Resource load(String filename);

    /**
     * delete all
     */
    public void deleteAll();

    /**
     * load all resource
     *
     * @return
     */
    public Stream<Path> loadAll();
}
