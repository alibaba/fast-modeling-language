package com.aliyun.fastmodel.transform.oceanbase.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * oceanbase char data type
 *
 * @author panguanjing
 * @date 2024/2/13
 */
@Getter
public class OceanBaseMysqlCharDataType extends GenericDataType {

    private final String dataTypeName;

    private final List<DataTypeParameter> arguments;

    private final CharsetKey charsetKey;

    private final String charsetName;

    private final String collation;

    public enum CharsetKey {
        CHARSET("CHARSET"),

        CHARACTER_SET("CHARACTER SET");
        @Getter
        String value;

        CharsetKey(String value) {
            this.value = value;
        }

    }

    public OceanBaseMysqlCharDataType(String dataTypeName, List<DataTypeParameter> parameters, CharsetKey charsetKey, String charsetName,
        String collation) {
        super(dataTypeName);
        this.dataTypeName = dataTypeName;
        this.arguments = parameters;
        this.charsetKey = charsetKey;
        this.charsetName = charsetName;
        this.collation = collation;
    }

    @Override
    public IDataTypeName getTypeName() {
        return OceanBaseMysqlDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> oceanBaseMysqlAstVisitor = (OceanBaseMysqlAstVisitor)visitor;
        return oceanBaseMysqlAstVisitor.visitOceanBaseCharDataType(this, context);
    }
}
