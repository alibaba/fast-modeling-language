package com.aliyun.fastmodel.transform.hive.parser.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.transform.hive.parser.HiveParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.Vocabulary;

/**
 * hologres reserved keyword
 *
 * @author panguanjing
 * @date 2023/1/27
 */
public class HiveReservedWordUtil {

    private static final Pattern KW_IDENTIFIER = Pattern.compile("(KW_)([A-Z0-9_]+)");

    private static final Set<String> SET = new HashSet<>();

    static {
        Vocabulary vocabulary = HiveParser.VOCABULARY;
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getSymbolicName(i));
            Matcher matcher = KW_IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                SET.add(matcher.group(2));
            }
        }
        //因为vocabulary中单独是定义
        SET.add("INTEGER");
        SET.add("NUMERIC");
    }

    public static boolean isReservedKeyWord(String word) {
        return SET.contains(word.toUpperCase());
    }
}
