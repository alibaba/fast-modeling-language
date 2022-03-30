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

package com.aliyun.fastmodel.transform.excel.spi;

import java.io.OutputStream;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;

/**
 * 定义一个支持excel读取和写入的plugin
 * 职责：
 * 1.  可以进行FML转换。
 * 2.  可以将FML导出为excel的模板
 *
 * @author panguanjing
 * @date 2021/5/16
 */
public interface Plugin<T extends BaseStatement, C> {

    /**
     * 将excel转换为FML模型
     *
     * @param source  source
     * @param context 上下文信息
     * @return {@link BaseStatement}
     * @throws Exception 如果转换中出现错误，会抛出异常
     */
    List<T> toFml(InputStreamSource source, PluginContext<C> context) throws Exception;

    /**
     * 转换为excel
     *
     * @param statement    FML
     * @param outputStream 输出的流信息
     * @param context      上下文信息
     */
    void toExcel(List<T> statement, OutputStream outputStream, PluginContext<C> context);

    /**
     * 根据contex来进行返回内容
     *
     * @param pluginDataSource
     * @param outputStream
     * @param context          context上下文信息
     */
    default void toExcelByFunction(PluginDataSource pluginDataSource, OutputStream outputStream,
                                   PluginContext<C> context) {
        List<T> apply = pluginDataSource.supply(context);
        toExcel(apply, outputStream, context);
    }

    /**
     * 根据QualifiedName导出excel模板
     *
     * @param qualifiedName 名称
     * @param outputStream  excel输出流
     * @param context       上下文信息
     */
    void toExcelByQualifiedName(List<QualifiedName> qualifiedName, OutputStream outputStream, PluginContext<C> context);

    /**
     * 返回pluginConfig
     *
     * @return
     */
    default PluginConfig getConfig() {
        return getClass().getAnnotation(PluginConfig.class);
    }

    /**
     * 插件的数据源信息内容
     *
     * @param context 获取上下文信息
     * @return
     */
    default PluginDataSource<T, C> getPluginDataSource(PluginContext<C> context) {
        throw new UnsupportedOperationException("unsupported plugin DataSource");
    }

}
