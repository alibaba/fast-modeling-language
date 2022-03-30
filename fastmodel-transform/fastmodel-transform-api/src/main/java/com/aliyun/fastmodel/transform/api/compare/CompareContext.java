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

package com.aliyun.fastmodel.transform.api.compare;

import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import lombok.Getter;
import lombok.ToString;

/**
 * 比对的上下文
 *
 * @author panguanjing
 * @date 2021/9/4
 */
@ToString
@Getter
public class CompareContext {

    private final QualifiedName qualifiedName;

    private final CompareStrategy compareStrategy;

    protected CompareContext(CompareContextBuilder builder) {
        qualifiedName = builder.qualifiedName;
        compareStrategy = builder.compareStrategy;
    }

    public static CompareContextBuilder builder() {
        return new CompareContextBuilder();
    }

    public static class CompareContextBuilder {
        private QualifiedName qualifiedName;

        private CompareStrategy compareStrategy = CompareStrategy.INCREMENTAL;

        public CompareContextBuilder qualifiedName(QualifiedName qualifiedName) {
            this.qualifiedName = qualifiedName;
            return this;
        }

        public CompareContextBuilder compareStrategy(CompareStrategy compareStrategy) {
            this.compareStrategy = compareStrategy;
            return this;
        }

        public CompareContext build() {
            return new CompareContext(this);
        }
    }
}
