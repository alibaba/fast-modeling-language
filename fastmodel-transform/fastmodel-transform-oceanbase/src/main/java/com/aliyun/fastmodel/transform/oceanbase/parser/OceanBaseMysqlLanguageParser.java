package com.aliyun.fastmodel.transform.oceanbase.parser;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBLexer;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * OceanBaseMysqlParser
 *
 * @author panguanjing
 * @date 2024/2/2
 */
@AutoService(LanguageParser.class)
public class OceanBaseMysqlLanguageParser implements LanguageParser<Node, ReverseContext> {

    @Override
    public <T> T parseNode(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new OBLexer(charStream),
            tokenStream -> new OBParser(tokenStream),
            parser -> {
                OBParser dorisParser = (OBParser)parser;
                return dorisParser.sql_stmt();
            }
        );
        return (T)parserRuleContext.accept(new OceanBaseMysqlAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new OBLexer(charStream),
            tokenStream -> new OBParser(tokenStream),
            parser -> {
                OBParser dorisParser = (OBParser)parser;
                return dorisParser.data_type();
            }
        );
        return (BaseDataType)parserRuleContext.accept(new OceanBaseMysqlAstBuilder(context));
    }

    @Override
    public <T> T parseExpression(String text) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new OBLexer(charStream),
            tokenStream -> new OBParser(tokenStream),
            parser -> {
                OBParser dorisParser = (OBParser)parser;
                return dorisParser.expr();
            }
        );
        return (T)parserRuleContext.accept(new OceanBaseMysqlAstBuilder(ReverseContext.builder().build()));
    }
}
