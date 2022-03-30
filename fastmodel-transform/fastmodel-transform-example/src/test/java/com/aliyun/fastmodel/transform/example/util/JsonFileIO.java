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

package com.aliyun.fastmodel.transform.example.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

/**
 * json的工具类
 *
 * @author panguanjing
 * @date 2021/6/15
 */
public class JsonFileIO {

    public static String getExpectValue(String type) {
        try {
            String name = "/expect_value_" + type + ".json";
            InputStream resourceAsStream = JsonFileIO.class.getResourceAsStream(name);
            return IOUtils.toString(resourceAsStream,
                StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("getResource occur exception:" + type, e);
        }
    }

}
