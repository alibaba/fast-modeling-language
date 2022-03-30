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

package com.aliyun.fastmodel.transform.excel.spi.source;

import java.io.FileInputStream;
import java.io.InputStream;

import com.aliyun.fastmodel.transform.excel.spi.InputStreamSource;

/**
 * FileResource
 *
 * @author panguanjing
 * @date 2021/5/16
 */
public class FileResource implements InputStreamSource {

    private final String fileName;

    public FileResource(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public InputStream getInputStream() throws Exception {
        return new FileInputStream(fileName);
    }

}
