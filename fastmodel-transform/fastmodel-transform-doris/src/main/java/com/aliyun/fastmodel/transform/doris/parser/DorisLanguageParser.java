package com.aliyun.fastmodel.transform.doris.parser;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.doris.parser.visitor.DorisAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * doris language parser
 *
 * @author panguanjing
 * @date 2024/1/20
 */
@AutoService(LanguageParser.class)
public class DorisLanguageParser implements LanguageParser<Node, ReverseContext> {
    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new DorisLexer(charStream),
            tokenStream -> new DorisParser(tokenStream),
            parser -> {
                DorisParser dorisParser = (DorisParser)parser;
                return dorisParser.multiStatements();
            }
        );
        return parserRuleContext.accept(new DorisAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new DorisLexer(charStream),
            tokenStream -> new DorisParser(tokenStream),
            parser -> {
                DorisParser dorisParser = (DorisParser)parser;
                return dorisParser.dataType();
            }
        );
        return (BaseDataType)parserRuleContext.accept(new DorisAstBuilder(context));
    }
}
