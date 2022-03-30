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

package com.aliyun.fastmodel.transform.api.context;

import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * TransformContext
 *
 * @author panguanjing
 * @date 2020/10/16
 */
@ToString
@Setter
@Getter
public class TransformContext {

    public static final String SEMICOLON = ";";

    /**
     * 是否append分号
     */
    private boolean appendSemicolon;

    /**
     * dataTypeTransformer, 类型转换器处理
     */
    private DataTypeConverter dataTypeTransformer;

    /**
     * 支持从另外一个context直接进行赋值
     *
     * @param context
     */
    public TransformContext(TransformContext context) {
        if (context == null) {
            return;
        }
        appendSemicolon = context.isAppendSemicolon();
        dataTypeTransformer = context.getDataTypeTransformer();
    }

    /**
     * 使用Builder进行传递
     *
     * @param tBuilder
     * @param <T>
     */
    protected <T extends Builder<T>> TransformContext(Builder tBuilder) {
        Preconditions.checkNotNull(tBuilder);
        appendSemicolon = tBuilder.appendSemicolon;
        dataTypeTransformer = tBuilder.dataTypeTransformer;
    }

    /**
     * 初始化builder
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder<T extends Builder<T>> {

        private boolean appendSemicolon;

        /**
         * 是否做语法校验
         */
        private boolean parseValid;

        private DataTypeConverter dataTypeTransformer;

        public T dataTypeTransformer(DataTypeConverter dataTypeTransformer) {
            this.dataTypeTransformer = dataTypeTransformer;
            return (T)this;
        }

        public T appendSemicolon(boolean append) {
            appendSemicolon = append;
            return (T)this;
        }

        public T parseValid(boolean valid) {
            parseValid = valid;
            return (T)this;
        }

        public TransformContext build() {
            return new TransformContext(this);
        }

    }

}
