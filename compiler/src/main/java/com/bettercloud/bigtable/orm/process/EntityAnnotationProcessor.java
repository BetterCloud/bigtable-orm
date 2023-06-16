package com.bettercloud.bigtable.orm.process;

import com.bettercloud.bigtable.orm.annotations.Entity;
import com.google.auto.service.AutoService;
import com.squareup.javapoet.JavaFile;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
public class EntityAnnotationProcessor extends AbstractProcessor {

  @Override
  public synchronized void init(final ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
  }

  @Override
  public boolean process(
      final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {
    final Messager messager = processingEnv.getMessager();

    final Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(Entity.class);

    final Elements elementUtils = processingEnv.getElementUtils();
    final EntitySourceGenerator entitySourceGenerator = new EntitySourceGenerator(elementUtils);

    elements.forEach(
        entityElement -> {
          try {
            final JavaFile javaFile = entitySourceGenerator.generateForElement(entityElement);

            try {
              javaFile.writeTo(processingEnv.getFiler());
            } catch (final IOException e) {
              messager.printMessage(
                  Diagnostic.Kind.ERROR, "An error occurred: " + e, entityElement);
            }
          } catch (final ElementProcessingException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage(), e.getElement());
          }
        });

    return true;
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return Collections.singleton(Entity.class.getCanonicalName());
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latestSupported();
  }
}
