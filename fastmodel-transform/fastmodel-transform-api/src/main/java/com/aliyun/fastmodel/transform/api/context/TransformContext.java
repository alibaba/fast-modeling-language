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

import com.aliyun.fastmodel.transform.api.context.setting.QuerySetting;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
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
     * database
     */
    private String database;

    /**
     * schema
     */
    private String schema;

    /**
     * dataTypeTransformer, 类型转换器处理
     */
    private DataTypeConverter dataTypeTransformer;

    /**
     * 转换view处理内容
     */
    private ViewSetting viewSetting = new ViewSetting();

    /**
     * query语句转换配置
     */
    private QuerySetting querySetting = new QuerySetting();

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
        this.viewSetting = context.getViewSetting();
        this.database = context.getDatabase();
        this.schema = context.getSchema();
        this.querySetting = context.getQuerySetting();
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
        viewSetting = tBuilder.viewSetting;
        this.querySetting = tBuilder.querySetting;
        this.database = tBuilder.database;
        this.schema = tBuilder.schema;
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
         * database
         */
        private String database;

        /**
         * schema
         */
        private String schema;

        /**
         * 数据类型转换
         */
        private DataTypeConverter dataTypeTransformer;

        /**
         * view转换设置
         */
        private ViewSetting viewSetting = new ViewSetting();

        /**
         * query语句转换配置
         */
        private QuerySetting querySetting = new QuerySetting();

        public T dataTypeTransformer(DataTypeConverter dataTypeTransformer) {
            this.dataTypeTransformer = dataTypeTransformer;
            return (T)this;
        }

        public T appendSemicolon(boolean append) {
            appendSemicolon = append;
            return (T)this;
        }

        public T transformToView(ViewSetting transformViewContext) {
            this.viewSetting = transformViewContext;
            return (T)this;
        }

        public T querySetting(QuerySetting querySetting) {
            this.querySetting = querySetting;
            return (T)this;
        }

        public T database(String database) {
            this.database = database;
            return (T)this;
        }

        public T schema(String schema) {
            this.schema = schema;
            return (T)this;
        }

        public TransformContext build() {
            return new TransformContext(this);
        }

    }

}
