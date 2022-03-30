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

package com.aliyun.fastmodel.ide.spi.params;

import java.util.Map;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 文本格式化的参数
 *
 * @author panguanjing
 * @date 2022/1/11
 */
@Getter
@Setter
@ToString
@Builder
public class DocumentFormatParams {
    private String text;
    private FormatOptions formatOptions;

    static class FormatOptions {
        private Map<String, String> maps = Maps.newHashMap();

        public void put(String key, String value) {
            maps.put(key, value);
        }

        public String get(String key) {
            return maps.get(key);
        }
    }
}
