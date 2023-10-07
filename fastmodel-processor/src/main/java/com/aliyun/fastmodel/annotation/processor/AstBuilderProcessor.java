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

package com.aliyun.fastmodel.annotation.processor;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic.Kind;

import com.google.auto.service.AutoService;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.commons.lang3.StringUtils;

/**
 * @author panguanjing
 * @date 2021/4/8
 */
@SupportedAnnotationTypes("com.aliyun.fastmodel.parser.annotation.SubVisitor")
@AutoService(Processor.class)
public class AstBuilderProcessor extends AbstractProcessor {
    private Filer filer;

    private Messager messager;

    private Elements elements;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        filer = processingEnv.getFiler();
        messager = processingEnv.getMessager();
        elements = processingEnv.getElementUtils();
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (!roundEnv.processingOver()) {
            for (TypeElement annotation : annotations) {
                if (annotation.getKind() != ElementKind.ANNOTATION_TYPE) {
                    continue;
                }
                Set<? extends Element> elementsAnnotatedWith = roundEnv.getElementsAnnotatedWith(annotation);
                Set<Element> executeElement = new HashSet<>();
                for (Element e : elementsAnnotatedWith) {
                    if (e.getKind() != ElementKind.CLASS) {
                        continue;
                    }
                    TypeElement element = (TypeElement)e;
                    List<? extends Element> enclosedElements = element.getEnclosedElements();
                    for (Element execute : enclosedElements) {
                        if (execute.getKind() != ElementKind.METHOD) {
                            continue;
                        }
                        ExecutableElement executableElement = (ExecutableElement)execute;
                        Annotation annotation1 = executableElement.getAnnotation(Override.class);
                        if (annotation1 == null) {
                            continue;
                        }
                        executeElement.add(executableElement);
                    }
                }
                process(executeElement);
            }

        }
        return false;
    }

    public void process(Set<? extends Element> executeElement) {
        List<MethodSpec> methodSpecs = new ArrayList<>();
        List<FieldSpec> fieldSpecs = new ArrayList<>();
        Element enclosingElement = null;
        for (Element e : executeElement) {
            ExecutableElement executableElement = (ExecutableElement)e;
            String name = e.getSimpleName().toString();
            Builder spec = MethodSpec.methodBuilder(name)
                .addAnnotation(Override.class)
                .addModifiers(e.getModifiers()).returns(
                    TypeName.get(executableElement.getReturnType())
                );
            int size = executableElement.getParameters().size();
            String name1 = null;
            for (int i = 0; i < size; i++) {
                VariableElement variableElement = executableElement.getParameters().get(i);
                name1 = variableElement.getSimpleName().toString();
                spec.addParameter(TypeName.get(variableElement.asType()),
                    name1);
            }
            enclosingElement = executableElement.getEnclosingElement();

            TypeMirror typeMirror = null;
            if (enclosingElement != null) {
                typeMirror = enclosingElement.asType();
            } else {
                messager.printMessage(Kind.ERROR, "enclosing element is null");
            }
            ClassName className = ClassName.get((TypeElement)enclosingElement);
            String capitalize = StringUtils.uncapitalize(className.simpleName());
            TypeName type = TypeName.get(typeMirror);
            FieldSpec fieldSpec = FieldSpec.builder(type, capitalize).addModifiers(Modifier.PRIVATE)
                .initializer(
                    "new $T()", type
                ).build();
            spec.addStatement("return $N.$L($N)", capitalize, name, name1);
            methodSpecs.add(spec.build());
            if (!fieldSpecs.contains(fieldSpec)) {
                fieldSpecs.add(fieldSpec);
            }
        }
        if (enclosingElement == null) {
            return;
        }
        try {
            write(methodSpecs, fieldSpecs, enclosingElement);
        } catch (IOException e) {
            messager.printMessage(Kind.ERROR, e.getMessage());
        }
    }

    public void write(List<MethodSpec> methodSpecs, List<FieldSpec> fieldSpecs,
                      Element enclosingElement) throws IOException {
        TypeElement typeElement = (TypeElement)enclosingElement;
        TypeSpec astBuilder = TypeSpec.classBuilder("AnnotationProcessVisitor")
            .superclass(typeElement.getSuperclass())
            .addModifiers(Modifier.PUBLIC)
            .addFields(fieldSpecs)
            .addMethods(methodSpecs)
            .build();

        PackageElement packageOf = elements.getPackageOf(enclosingElement);
        JavaFile javaFile = JavaFile.builder(packageOf.getQualifiedName().toString(), astBuilder)
            .build();

        javaFile.writeTo(filer);
    }
}
