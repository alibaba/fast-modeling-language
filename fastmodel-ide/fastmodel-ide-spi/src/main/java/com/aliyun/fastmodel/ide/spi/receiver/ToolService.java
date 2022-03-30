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

package com.aliyun.fastmodel.ide.spi.receiver;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

/**
 * 工具服务，用于与外界进行交互的服务。
 * <pre>
 *     导入服务：从其他方言的内容机型导入
 *     导出服务：将当前的Fml语句进行导出
 * </pre>
 *
 * @author panguanjing
 * @date 2022/1/12
 */
public interface ToolService {
    /**
     * import sql to convert fml
     *
     * @param text        sql
     * @param dialectMeta 方言
     * @return 导入sql转换为fml
     */
    public String importSql(String text, DialectMeta dialectMeta);

    /**
     * export sql
     *
     * @param text        sql
     * @param dialectMeta 方言
     * @return
     */
    public String exportSql(String text, DialectMeta dialectMeta);

    /**
     * uri的信息内容
     *
     * @param uri
     * @param dialectMeta
     * @return
     */
    String importByUri(String uri, DialectMeta dialectMeta);
}
