/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aliyun.fastmodel.agent.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Non-interactive FML interface intended for agents and automation.
 */
@Command(
    name = "fml",
    mixinStandardHelpOptions = true,
    versionProvider = FmlAgentCli.VersionProvider.class,
    description = "Parse, validate, inspect, format, and transform Fast Modeling Language.",
    subcommands = {
        FmlAgentCli.Validate.class,
        FmlAgentCli.Format.class,
        FmlAgentCli.Transform.class,
        FmlAgentCli.Inspect.class,
        FmlAgentCli.Capabilities.class
    }
)
public final class FmlAgentCli implements Runnable {

    static final int INVALID_INPUT = 2;
    static final int PARSE_ERROR = 3;
    static final int TRANSFORM_ERROR = 4;

    FmlAgentCli() {
    }

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new FmlAgentCli());
        commandLine.setExecutionExceptionHandler((exception, cmd, parseResult) -> {
            Throwable cause = rootCause(exception);
            int exitCode;
            if (cause instanceof ParseException) {
                exitCode = PARSE_ERROR;
            } else if (cause instanceof IllegalArgumentException) {
                exitCode = INVALID_INPUT;
            } else {
                exitCode = TRANSFORM_ERROR;
            }
            System.out.println(errorJson(cause, exitCode));
            return exitCode;
        });
        commandLine.setParameterExceptionHandler((exception, args1) -> {
            System.out.println(errorJson(exception, INVALID_INPUT));
            return INVALID_INPUT;
        });
        System.exit(commandLine.execute(args));
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    /**
     * 从打包产物解析 CLI 版本号。
     * <p>
     * 注解占位符（如 ${revision}）在编译期不会被展开，直接写死会输出 null；
     * 改为运行时优先读 manifest 的 Implementation-Version，其次读 jar 内的 pom.properties。
     */
    static final class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {"fml-agent-cli " + resolveVersion()};
        }

        static String resolveVersion() {
            // 隔离 ClassLoader（如测试框架）下 getPackage() 可能为 null，需防御
            Package pkg = FmlAgentCli.class.getPackage();
            String manifestVersion = pkg == null ? null : pkg.getImplementationVersion();
            if (manifestVersion != null && !manifestVersion.trim().isEmpty()) {
                return manifestVersion.trim();
            }
            try (java.io.InputStream input = FmlAgentCli.class.getResourceAsStream(
                "/META-INF/maven/com.aliyun.fastmodel/fastmodel-agent-cli/pom.properties")) {
                if (input != null) {
                    java.util.Properties properties = new java.util.Properties();
                    properties.load(input);
                    String version = properties.getProperty("version");
                    if (version != null && !version.trim().isEmpty()) {
                        return version.trim();
                    }
                }
            } catch (IOException ignored) {
                // 无法读取时回退为 unknown，保证 --version 输出可解析
            }
            return "unknown";
        }
    }

    abstract static class InputCommand implements Callable<Integer> {
        @Parameters(index = "0", arity = "0..1", paramLabel = "FILE",
            description = "FML file. Reads stdin when omitted or when FILE is '-'.")
        private String file;

        @Option(names = "--text", description = "Inline FML text. Cannot be combined with FILE.")
        private String text;

        protected String readInput() throws IOException {
            if (text != null && file != null) {
                throw new IllegalArgumentException("--text cannot be combined with FILE");
            }
            if (text != null) {
                return text;
            }
            if (file == null || "-".equals(file)) {
                byte[] bytes = readAllBytes();
                if (bytes.length == 0) {
                    throw new IllegalArgumentException("FML input is empty");
                }
                return new String(bytes, StandardCharsets.UTF_8);
            }
            Path path = Paths.get(file);
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException("FML file does not exist: " + file);
            }
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        }

        protected List<BaseStatement> parse() throws IOException {
            FastModelParser parser = FastModelParserFactory.getInstance().get();
            return parser.multiParse(new DomainLanguage(readInput()));
        }
    }

    @Command(name = "validate", mixinStandardHelpOptions = true,
        description = "Validate FML or a supported engine dialect and return structured diagnostics.")
    static final class Validate extends InputCommand {
        @Option(names = {"-d", "--dialect"}, defaultValue = "fml",
            description = "Input dialect. Defaults to fml; use `fml capabilities` for supported values.")
        private String dialect;

        @Override
        public Integer call() throws Exception {
            String target = normalizeDialect(dialect);
            // 部分方言的 reverse() 返回非 BaseStatement 的 Node 子类型（如 zen 的 ListNode），统一按 Node 处理
            List<Node> statements;
            if ("fml".equals(target)) {
                statements = new ArrayList<>(parse());
            } else {
                Transformer<BaseStatement> transformer = requireTransformer(target);
                if (!supportsNativeValidation(target, transformer)) {
                    throw new IllegalArgumentException(
                        "Native validation is not available for dialect: " + target);
                }
                Node statement = transformer.reverse(
                    new DialectNode(readInput()), ReverseContext.builder().build());
                if (statement == null) {
                    throw new UnsupportedOperationException(
                        "Dialect parser returned no statement for: " + target);
                }
                statements = Collections.singletonList(statement);
            }
            String types = statements.stream()
                .map(statement -> quote(statement.getClass().getSimpleName()))
                .collect(Collectors.joining(","));
            System.out.println("{\"ok\":true,\"statementCount\":" + statements.size()
                + ",\"dialect\":" + quote(target) + ",\"statementTypes\":[" + types
                + "],\"diagnostics\":[]}");
            return 0;
        }
    }

    @Command(name = "format", mixinStandardHelpOptions = true,
        description = "Format FML source using the canonical FML transformer.")
    static final class Format extends InputCommand {
        @Override
        public Integer call() throws Exception {
            String result = transformStatements(parse(), "fml");
            System.out.println("{\"ok\":true,\"dialect\":\"fml\",\"output\":" + quote(result) + "}");
            return 0;
        }
    }

    @Command(name = "transform", mixinStandardHelpOptions = true,
        description = "Transform FML into a supported target dialect.")
    static final class Transform extends InputCommand {
        @Option(names = {"-d", "--dialect"}, required = true,
            description = "Target dialect from `fml capabilities`; use name@version for versioned dialects.")
        private String dialect;

        @Override
        public Integer call() throws Exception {
            String target = normalizeDialect(dialect);
            String result = transformStatements(parse(), target);
            System.out.println("{\"ok\":true,\"dialect\":" + quote(target)
                + ",\"output\":" + quote(result) + "}");
            return 0;
        }
    }

    @Command(name = "inspect", mixinStandardHelpOptions = true,
        description = "Inspect the parsed statement tree as stable JSON.")
    static final class Inspect extends InputCommand {
        @Override
        public Integer call() throws Exception {
            List<BaseStatement> statements = parse();
            String nodes = statements.stream()
                .map(FmlAgentCli::nodeJson)
                .collect(Collectors.joining(","));
            System.out.println("{\"ok\":true,\"statementCount\":" + statements.size()
                + ",\"statements\":[" + nodes + "]}");
            return 0;
        }
    }

    @Command(name = "capabilities", mixinStandardHelpOptions = true,
        description = "Describe commands, dialects, input modes, and exit codes.")
    static final class Capabilities implements Callable<Integer> {
        @Override
        public Integer call() {
            String dialects = transformerRegistry().entrySet().stream()
                .map(entry -> dialectCapabilityJson(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
            System.out.println("{\"ok\":true,\"commands\":[\"validate\",\"format\",\"transform\","
                + "\"inspect\",\"capabilities\"],\"dialects\":[" + dialects + "],"
                + "\"inputModes\":[\"file\",\"stdin\",\"inline\"],"
                + "\"exitCodes\":{\"success\":0,\"invalidInput\":2,\"parseError\":3,\"transformError\":4}}");
            return 0;
        }
    }

    private static String transformStatements(List<BaseStatement> statements, String dialect) {
        String target = normalizeDialect(dialect);
        Transformer<BaseStatement> transformer = requireTransformer(target);
        TransformContext context = TransformContext.builder()
            .prettyFormat(true)
            .appendSemicolon(true)
            .build();
        if ("graph".equals(target) || "plantuml".equals(target)) {
            DialectNode transformed = transformer.transform(new CompositeStatement(statements), context);
            return requireOutput(transformed, "CompositeStatement");
        }
        List<String> output = new ArrayList<>();
        for (BaseStatement statement : statements) {
            DialectNode transformed = transformer.transform(statement, context);
            output.add(requireOutput(transformed, statement.getClass().getSimpleName()));
        }
        return String.join(System.lineSeparator(), output);
    }

    private static Transformer<BaseStatement> requireTransformer(String dialect) {
        Transformer<BaseStatement> transformer = transformerRegistry().get(normalizeDialect(dialect));
        if (transformer == null) {
            throw new IllegalArgumentException("Unsupported dialect: " + dialect
                + ". Run `fml capabilities` to list packaged transformers.");
        }
        return transformer;
    }

    private static String requireOutput(DialectNode transformed, String statementType) {
        if (transformed == null || transformed.getNode() == null) {
            throw new IllegalStateException("Transformer returned no output for " + statementType);
        }
        return transformed.getNode();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Transformer<BaseStatement>> transformerRegistry() {
        Map<String, Transformer<BaseStatement>> discovered = new java.util.TreeMap<>();
        for (Transformer<?> transformer : ServiceLoader.load(Transformer.class)) {
            Dialect dialect = transformer.getClass().getAnnotation(Dialect.class);
            if (dialect == null) {
                continue;
            }
            String name = normalizeDialect(dialect.value());
            String version = dialect.version().trim();
            Transformer<BaseStatement> typed = (Transformer<BaseStatement>)transformer;
            if (version.isEmpty() || dialect.defaultDialect()) {
                discovered.put(name, typed);
            }
            if (!version.isEmpty()) {
                discovered.put(name + "@" + version.toLowerCase(Locale.ENGLISH), typed);
            }
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(discovered));
    }

    private static String normalizeDialect(String dialect) {
        return dialect.trim().toLowerCase(Locale.ENGLISH).replace('_', '-');
    }

    private static String dialectCapabilityJson(String name, Transformer<BaseStatement> transformer) {
        return "{\"name\":" + quote(name)
            + ",\"transform\":" + advertisedTransform(transformer)
            + ",\"validate\":" + supportsNativeValidation(name, transformer) + "}";
    }

    static boolean advertisedTransform(Transformer<BaseStatement> transformer) {
        return overrides(transformer, "transform", Node.class, TransformContext.class);
    }

    static boolean supportsNativeValidation(String name, Transformer<BaseStatement> transformer) {
        // PostgreSQL exposes reverse(), but its parser currently returns null for valid CREATE TABLE input.
        if ("postgresql".equals(name)) {
            return false;
        }
        return "fml".equals(name)
            || overrides(transformer, "reverse", DialectNode.class, ReverseContext.class);
    }

    private static boolean overrides(Transformer<?> transformer, String method, Class<?>... parameterTypes) {
        try {
            return transformer.getClass().getMethod(method, parameterTypes).getDeclaringClass() != Transformer.class;
        } catch (NoSuchMethodException exception) {
            return false;
        }
    }

    private static String nodeJson(Node node) {
        String children = node.getChildren().stream()
            .map(FmlAgentCli::nodeJson)
            .collect(Collectors.joining(","));
        return "{\"type\":" + quote(node.getClass().getSimpleName())
            + ",\"children\":[" + children + "]}";
    }

    private static byte[] readAllBytes() throws IOException {
        byte[] buffer = new byte[8192];
        int read;
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        while ((read = System.in.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable result = throwable;
        while (result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
        }
        return result;
    }

    private static String errorJson(Throwable throwable, int exitCode) {
        int line = 0;
        int column = 0;
        if (throwable instanceof ParseException) {
            line = ((ParseException)throwable).getLine();
            column = ((ParseException)throwable).getColumn();
        }
        String code;
        if (exitCode == PARSE_ERROR) {
            code = "FML_PARSE_ERROR";
        } else if (exitCode == INVALID_INPUT) {
            code = "FML_INVALID_INPUT";
        } else {
            code = "FML_EXECUTION_ERROR";
        }
        return "{\"ok\":false,\"diagnostics\":[{\"code\":" + quote(code)
            + ",\"message\":" + quote(String.valueOf(throwable.getMessage()))
            + ",\"line\":" + line + ",\"column\":" + column + "}]}";
    }

    static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        escaped.append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (character < 0x20) {
                        escaped.append(String.format("\\u%04x", (int)character));
                    } else {
                        escaped.append(character);
                    }
            }
        }
        return escaped.append('"').toString();
    }
}
