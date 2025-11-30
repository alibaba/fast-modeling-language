package com.aliyun.fastmodel.transform.mysql.client.converter;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.aliyun.fastmodel.transform.mysql.format.MysqlVisitor;
import com.aliyun.fastmodel.transform.mysql.parser.MysqlTransformerParser;
import com.aliyun.fastmodel.transform.mysql.parser.tree.MysqlDataTypeName;

/**
 * MySQL客户端转换器
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public class MysqlClientConverter extends ExtensionClientConverter<MysqlTransformContext> {

    private final MysqlTransformerParser mysqlTransformerParser;

    private final MysqlPropertyConverter mysqlPropertyConverter;

    public MysqlClientConverter() {
        mysqlTransformerParser = new MysqlTransformerParser();
        mysqlPropertyConverter = new MysqlPropertyConverter();
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.mysqlTransformerParser;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return mysqlPropertyConverter;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        try {
            return MysqlDataTypeName.getByValue(dataTypeName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String getRaw(Node node) {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());
        node.accept(mysqlVisitor, 0);
        return mysqlVisitor.getBuilder().toString();
    }
}