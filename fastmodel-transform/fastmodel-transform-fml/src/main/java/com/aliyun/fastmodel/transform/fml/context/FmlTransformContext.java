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

package com.aliyun.fastmodel.transform.fml.context;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;

/**
 * FmlTransformContext
 *
 * @author panguanjing
 * @date 2021/3/9
 */
@Getter
public class FmlTransformContext extends TransformContext {

    /**
     * 需要过滤的属性map
     */
    private Map<String, Boolean> filterMap = new HashMap<>();

    public FmlTransformContext(TransformContext context) {
        super(context);
        if (context instanceof FmlTransformContext) {
            FmlTransformContext parent = (FmlTransformContext)context;
            filterMap = parent.getFilterMap();
        }
    }

    public FmlTransformContext(Builder tBuilder) {
        super(tBuilder);
        filterMap = tBuilder.filterKeys;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isFilter(String key) {
        Boolean filter = filterMap.get(key);
        return BooleanUtils.isTrue(filter);
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        /**
         * 默认为true
         */
        private Map<String, Boolean> filterKeys = Maps.newHashMap();

        public Builder exclude(String key) {
            filterKeys.put(key, true);
            return this;
        }

        public Builder include(String key) {
            filterKeys.put(key, false);
            return this;
        }

        @Override
        public FmlTransformContext build() {
            return new FmlTransformContext(this);
        }
    }

}

