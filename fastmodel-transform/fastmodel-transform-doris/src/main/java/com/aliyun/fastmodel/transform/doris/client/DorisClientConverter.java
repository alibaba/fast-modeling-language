package com.aliyun.fastmodel.transform.doris.client;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.format.DorisOutVisitor;
import com.aliyun.fastmodel.transform.doris.parser.DorisLanguageParser;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisDataTypeName;

/**
 * DorisClientConverter
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public class DorisClientConverter extends ExtensionClientConverter<DorisContext> {

    private final DorisLanguageParser dorisLanguageParser;

    private final DorisPropertyConverter dorisPropertyConverter;

    public DorisClientConverter() {
        dorisLanguageParser = new DorisLanguageParser();
        dorisPropertyConverter = new DorisPropertyConverter();
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return DorisDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.dorisLanguageParser;
    }

    @Override
    public String getRaw(Node node) {
        DorisOutVisitor dorisOutVisitor = new DorisOutVisitor(DorisContext.builder().build());
        node.accept(dorisOutVisitor, 0);
        return dorisOutVisitor.getBuilder().toString();
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return dorisPropertyConverter;
    }
}
