package com.aliyun.fastmodel.transform.starrocks.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;

/**
 * StarRocksLanguageParser
 *
 * @author panguanjing
 * @date 2023/9/5
 */
@AutoService(LanguageParser.class)
public class StarRocksLanguageParser implements LanguageParser<Node, ReverseContext> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, StarRocksParser::sqlStatements);
    }

    private Node getNode(String text, ReverseContext context, Function<StarRocksParser, ParserRuleContext> functionalInterface) {
        String code = StripUtils.appendSemicolon(text);
        CodePointCharStream charStream = CharStreams.fromString(code);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        StarRocksLexer lexer = new StarRocksLexer(caseChangingCharStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        StarRocksParser sparkParser = new StarRocksParser(commonTokenStream);
        sparkParser.removeErrorListeners();
        sparkParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            sparkParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = functionalInterface.apply(sparkParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            sparkParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = functionalInterface.apply(sparkParser);
        }
        return tree.accept(new StarRocksAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(code, context, StarRocksParser::type);
    }
}
