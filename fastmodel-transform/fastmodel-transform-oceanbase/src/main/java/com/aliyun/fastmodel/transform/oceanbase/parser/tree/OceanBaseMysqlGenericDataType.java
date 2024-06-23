package com.aliyun.fastmodel.transform.oceanbase.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * OceanBaseMysqlGenericDataType
 *
 * @author panguanjing
 * @date 2024/2/2
 */
@Getter
public class OceanBaseMysqlGenericDataType extends GenericDataType {

    public enum SignEnum {
        UNSIGNED,
        SIGNED
    }

    private final Boolean zeroFill;

    private final SignEnum signEnum;

    public OceanBaseMysqlGenericDataType(String dataTypeName,
        List<DataTypeParameter> arguments, Boolean zeroFill, SignEnum signEnum) {
        super(dataTypeName, arguments);
        this.zeroFill = zeroFill;
        this.signEnum = signEnum;
    }

    public OceanBaseMysqlGenericDataType(Identifier name,
        List<DataTypeParameter> arguments, Boolean zeroFill, SignEnum signEnum) {
        super(name, arguments);
        this.zeroFill = zeroFill;
        this.signEnum = signEnum;
    }

    @Override
    public IDataTypeName getTypeName() {
        return OceanBaseMysqlDataTypeName.getByValue(this.getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> oceanBaseMysqlAstVisitor = (OceanBaseMysqlAstVisitor)visitor;
        return oceanBaseMysqlAstVisitor.visitOceanBaseMysqlGenericDataType(this, context);
    }
}
