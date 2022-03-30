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

package com.aliyun.fastmodel.ide.spi.receiver.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.ide.spi.exception.PlatformException;
import com.aliyun.fastmodel.ide.spi.exception.error.PlatformErrorCode;
import com.aliyun.fastmodel.ide.spi.receiver.ToolService;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext.ReverseTargetStrategy;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 默认的工具服务
 *
 * @author panguanjing
 * @date 2022/1/12
 */
public class DefaultToolServiceImpl implements ToolService {

    private final TransformerFactory transformerFactory;

    private final FastModelParser fastModelParser;

    public DefaultToolServiceImpl() {
        this.transformerFactory = TransformerFactory.getInstance();
        this.fastModelParser = FastModelParserFactory.getInstance().get();
    }

    @Override
    public String importSql(String text, DialectMeta dialectMeta) {
        Transformer<Node> transformer = transformerFactory.get(dialectMeta);
        DialectNode dialectNode = new DialectNode(text);
        return transformer.reverse(dialectNode,
            ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build()).toString();
    }

    @Override
    public String exportSql(String fml, DialectMeta dialectMeta) {
        Transformer<Node> transformer = transformerFactory.get(dialectMeta);
        Node node = fastModelParser.parseStatement(fml);
        DialectNode dialectNode = transformer.transform(node);
        return dialectNode.getNode();
    }

    @Override
    public String importByUri(String uri, DialectMeta dialectMeta) {
        try {
            URI url = new URI(uri);
            String text = IOUtils.toString(url, UTF_8);
            return importSql(text, dialectMeta);
        } catch (IOException e) {
            throw new PlatformException("import by uri error" + uri, PlatformErrorCode.READ_FILE_ERROR, e);
        } catch (URISyntaxException e) {
            throw new PlatformException("import by uri error" + uri, PlatformErrorCode.URL_INVALID_ERROR, e);
        }
    }
}
