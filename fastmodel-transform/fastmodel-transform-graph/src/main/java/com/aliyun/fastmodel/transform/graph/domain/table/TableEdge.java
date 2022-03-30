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

package com.aliyun.fastmodel.transform.graph.domain.table;

import java.util.Map;
import java.util.Set;

import com.aliyun.fastmodel.transform.graph.domain.Edge;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import lombok.Builder;

/**
 * 表关系边类型
 *
 * @author panguanjing
 * @date 2021/12/12
 */
@Builder
public class TableEdge implements Edge {

    public static final String LEFT = "left";
    public static final String LEFT_COMMENT = "left_comment";
    public static final String RIGHT = "right";
    public static final String RIGHT_COMMENT = "right_comment";

    private final String source;

    private final String target;

    private final Map<String, Object> maps;
    private final String id;
    private final String label;

    @Override
    @JsonProperty("source")
    public String source() {
        return source;
    }

    @Override
    @JsonProperty("target")
    public String target() {
        return target;
    }

    @Override
    @JsonProperty("id")
    public String id() {
        return id;
    }

    @Override
    @JsonProperty("label")
    public String label() {
        return label;
    }

    @Override
    @JsonProperty("attribute")
    public Map<String, Object> properties() {
        return maps;
    }

    @Override
    public <T> T getProperty(String property) {
        return (T)maps.get(property);
    }

    @Override
    public void setProperty(String key, Object value) {
        maps.put(key, value);
    }

    @Override
    public void removeProperty(String key) {
        maps.remove(key);
    }

    @Override
    @JsonIgnore
    public Set<String> getPropertyKeys() {
        return Sets.newHashSet(LEFT, RIGHT, LEFT_COMMENT, RIGHT_COMMENT);
    }
}
