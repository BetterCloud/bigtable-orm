package com.bettercloud.bigtable.orm.process;

import com.bettercloud.bigtable.orm.EntityConfiguration;
import com.bettercloud.bigtable.orm.Key;
import com.bettercloud.bigtable.orm.KeyBuilder;
import com.bettercloud.bigtable.orm.RegisterableEntity;
import com.bettercloud.bigtable.orm.StringKey;
import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.CaseFormat;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import java.lang.annotation.Annotation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

class EntitySourceGenerator {

  private static final String INDENT = "    ";

  private final Elements elementUtils;

  EntitySourceGenerator(final Elements elementUtils) {
    this.elementUtils = elementUtils;
  }

  <T extends Element> JavaFile generateForElement(final T entityElement)
      throws ElementProcessingException {
    final Element tableElement =
        Optional.ofNullable(entityElement.getEnclosingElement())
            .filter(element -> element.getKind().isClass())
            .orElseThrow(
                () ->
                    new ElementProcessingException(
                        "@Entity types must be nested classes", entityElement));

    final Table table =
        Optional.ofNullable(tableElement.getAnnotation(Table.class))
            .orElseThrow(
                () ->
                    new ElementProcessingException(
                        "@Entity types must be nested classes of a parent annotated with @Table",
                        entityElement));

    if (tableElement.getModifiers().contains(Modifier.PUBLIC)) {
      throw new ElementProcessingException("@Table types must not be public", tableElement);
    }

    final String tableName =
        Optional.of(table.value())
            .filter(value -> !value.isEmpty())
            .orElseThrow(
                () -> new ElementProcessingException("@Table value must be defined", tableElement));

    if (!entityElement.getModifiers().contains(Modifier.PRIVATE)) {
      throw new ElementProcessingException("@Entity types must be private", entityElement);
    }

    if (entityElement.getModifiers().contains(Modifier.STATIC)) {
      throw new ElementProcessingException("@Entity types must not be static", entityElement);
    }

    final String packageName =
        Optional.of(elementUtils.getPackageOf(entityElement))
            .map(PackageElement::getQualifiedName)
            .map(Name::toString)
            .orElseThrow(IllegalStateException::new);

    final String entityName =
        Optional.of(entityElement.getSimpleName())
            .map(Name::toString)
            .orElseThrow(IllegalStateException::new);

    final ClassName entityClassName = ClassName.get(packageName, entityName);
    final TypeSpec.Builder entityBuilder =
        TypeSpec.classBuilder(entityClassName)
            .addModifiers(Modifier.PUBLIC)
            .superclass(RegisterableEntity.class);

    final MethodSpec.Builder equalsBuilder =
        MethodSpec.methodBuilder("equals")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addParameter(Object.class, "o", Modifier.FINAL)
            .beginControlFlow("if (this == $N)", "o")
            .addStatement("return true")
            .endControlFlow()
            .beginControlFlow("if ($N == null || getClass() != $N.getClass())", "o", "o")
            .addStatement("return false")
            .endControlFlow()
            .addStatement("final $T that = ($T) $N", entityClassName, entityClassName, "o");

    final CodeBlock.Builder equalsReturnBuilder = CodeBlock.builder().add("return ");

    final MethodSpec.Builder hashCodeBuilder =
        MethodSpec.methodBuilder("hashCode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.INT);

    final List<String> arrayFields = new ArrayList<>();
    final List<String> objectFields = new ArrayList<>();

    final MethodSpec.Builder toStringBuilder =
        MethodSpec.methodBuilder("toString")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class);

    final CodeBlock.Builder toStringReturnBuilder =
        CodeBlock.builder().add("return \"$N{\"", entityName);

    final ClassName columnsClassName = entityClassName.nestedClass("Columns");
    final TypeSpec.Builder columnsBuilder =
        TypeSpec.enumBuilder(columnsClassName)
            .addModifiers(Modifier.PRIVATE)
            .addSuperinterface(com.bettercloud.bigtable.orm.Column.class);

    final ClassName configurationClassName = entityClassName.nestedClass("Configuration");
    final TypeSpec.Builder entityConfigurationBuilder =
        TypeSpec.classBuilder(configurationClassName)
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .addSuperinterface(
                ParameterizedTypeName.get(
                    ClassName.get(EntityConfiguration.class), entityClassName))
            .addField(
                FieldSpec.builder(
                        ParameterizedTypeName.get(
                            ClassName.get(EntityConfiguration.class), entityClassName),
                        "INSTANCE",
                        Modifier.PRIVATE,
                        Modifier.STATIC,
                        Modifier.FINAL)
                    .initializer("new $T()", configurationClassName)
                    .build())
            .addField(
                FieldSpec.builder(
                        String.class,
                        "TABLE_NAME",
                        Modifier.PRIVATE,
                        Modifier.STATIC,
                        Modifier.FINAL)
                    .initializer("$S", tableName)
                    .build())
            .addField(
                FieldSpec.builder(
                        ParameterizedTypeName.get(
                            List.class, com.bettercloud.bigtable.orm.Column.class),
                        "COLUMNS",
                        Modifier.PRIVATE,
                        Modifier.STATIC,
                        Modifier.FINAL)
                    .initializer("$T.asList($T.values())", Arrays.class, columnsClassName)
                    .build())
            .addField(
                FieldSpec.builder(
                        ParameterizedTypeName.get(ClassName.get(Supplier.class), entityClassName),
                        "FACTORY",
                        Modifier.PRIVATE,
                        Modifier.STATIC,
                        Modifier.FINAL)
                    .initializer("$T::new", entityClassName)
                    .build())
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addComment("Prevent external instantiation")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getDefaultTableName")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(String.class)
                    .addStatement("return $N", "TABLE_NAME")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getColumns")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(
                        ParameterizedTypeName.get(
                            Iterable.class, com.bettercloud.bigtable.orm.Column.class))
                    .addStatement("return $N", "COLUMNS")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getEntityFactory")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(
                        ParameterizedTypeName.get(ClassName.get(Supplier.class), entityClassName))
                    .addStatement("return $N", "FACTORY")
                    .build());

    final ClassName delegateClassName = entityClassName.nestedClass("Delegate");
    final TypeSpec.Builder entityDelegateBuilder =
        TypeSpec.classBuilder(delegateClassName)
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .addSuperinterface(
                ParameterizedTypeName.get(
                    ClassName.get(EntityConfiguration.EntityDelegate.class), entityClassName))
            .addField(entityClassName, "entity", Modifier.PRIVATE, Modifier.FINAL)
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(entityClassName, "entity", Modifier.FINAL)
                    .addStatement("this.$N = $N", "entity", "entity")
                    .build());

    final MethodSpec.Builder getColumnValueBuilder =
        MethodSpec.methodBuilder("getColumnValue")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Object.class)
            .addParameter(com.bettercloud.bigtable.orm.Column.class, "column", Modifier.FINAL)
            .addStatement("$T.requireNonNull(column)", Objects.class);

    final MethodSpec.Builder setColumnValueBuilder =
        MethodSpec.methodBuilder("setColumnValue")
            .addAnnotation(
                AnnotationSpec.builder(SuppressWarnings.class)
                    .addMember("value", "$S", "unchecked")
                    .build())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(com.bettercloud.bigtable.orm.Column.class, "column", Modifier.FINAL)
            .addParameter(Object.class, "value", Modifier.FINAL);

    final MethodSpec.Builder getColumnTimestampBuilder =
        MethodSpec.methodBuilder("getColumnTimestamp")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Long.class)
            .addParameter(com.bettercloud.bigtable.orm.Column.class, "column", Modifier.FINAL)
            .addStatement("$T.requireNonNull(column)", Objects.class);

    final MethodSpec.Builder setColumnTimestampBuilder =
        MethodSpec.methodBuilder("setColumnTimestamp")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(com.bettercloud.bigtable.orm.Column.class, "column", Modifier.FINAL)
            .addParameter(Long.class, "timestamp", Modifier.FINAL);

    final Map<Element, Column> columnElements =
        Optional.ofNullable(entityElement.getEnclosedElements())
            .orElseGet(Collections::emptyList)
            .stream()
            .filter(enclosedElement -> enclosedElement.getAnnotation(Column.class) != null)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    enclosedElement -> enclosedElement.getAnnotation(Column.class),
                    (u, v) -> {
                      // Impossible
                      throw new IllegalStateException("Duplicate key: " + u);
                    },
                    LinkedHashMap::new // Preserves order of declared columns
                    ));

    if (columnElements.isEmpty()) {
      throw new ElementProcessingException(
          "@Entity types must declare at least one @Column field", entityElement);
    }

    final AtomicBoolean startedControlFlow = new AtomicBoolean(false);
    final AtomicBoolean startedTimestampControlFlow = new AtomicBoolean(false);

    final Set<Integer> columnHashes = new HashSet<>();

    for (final Map.Entry<Element, Column> entry : columnElements.entrySet()) {
      final Element columnElement = entry.getKey();
      final Column column = entry.getValue();

      if (columnElement.getModifiers().contains(Modifier.STATIC)) {
        throw new ElementProcessingException(
            "Static fields are not supported for @Column definitions", columnElement);
      }

      if (columnElement.getModifiers().contains(Modifier.FINAL)) {
        throw new ElementProcessingException(
            "Final fields are not supported for @Column definitions", columnElement);
      }

      final String lowerCamelCase = columnElement.getSimpleName().toString();
      final String upperCamelCase =
          CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, lowerCamelCase);
      final String upperCase =
          CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, lowerCamelCase);

      final TypeMirror typeMirror = columnElement.asType();
      final TypeKind kind = typeMirror.getKind();
      final TypeName typeName = TypeName.get(typeMirror).box();

      entityBuilder.addField(typeName, lowerCamelCase, Modifier.PRIVATE);

      final String getter = "get" + upperCamelCase;
      final String setter = "set" + upperCamelCase;

      entityBuilder.addMethod(
          MethodSpec.methodBuilder(getter)
              .addModifiers(Modifier.PUBLIC)
              .returns(typeName)
              .addStatement("return $N", lowerCamelCase)
              .build());

      final MethodSpec.Builder setterBuilder =
          MethodSpec.methodBuilder(setter)
              .addModifiers(Modifier.PUBLIC)
              .returns(TypeName.VOID)
              .addParameter(typeName, lowerCamelCase, Modifier.FINAL)
              .addStatement("this.$N = $N", lowerCamelCase, lowerCamelCase);

      if (column.versioned()) {
        final String timestampField = lowerCamelCase + "Timestamp";

        entityBuilder.addField(Long.class, timestampField, Modifier.PRIVATE);

        final String timestampGetter = "get" + upperCamelCase + "Timestamp";
        final String timestampSetter = "set" + upperCamelCase + "Timestamp";

        entityBuilder.addMethod(
            MethodSpec.methodBuilder(timestampGetter)
                .addModifiers(Modifier.PUBLIC)
                .returns(Long.class)
                .addStatement("return $N", timestampField)
                .build());

        setterBuilder.addStatement("this.$N = null", timestampField);

        entityBuilder.addMethod(
            MethodSpec.methodBuilder(setter)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(typeName, lowerCamelCase, Modifier.FINAL)
                .addParameter(TypeName.LONG, "timestamp", Modifier.FINAL)
                .addStatement("this.$N = $N", lowerCamelCase, lowerCamelCase)
                .addStatement("this.$N = $N", timestampField, "timestamp")
                .build());

        entityBuilder.addMethod(
            MethodSpec.methodBuilder(timestampSetter)
                .addModifiers(Modifier.PRIVATE)
                .returns(TypeName.VOID)
                .addParameter(Long.class, "timestamp", Modifier.FINAL)
                .addStatement("this.$N = $N", timestampField, "timestamp")
                .build());

        if (!startedTimestampControlFlow.getAndSet(true)) {
          getColumnTimestampBuilder
              .beginControlFlow("if ($T.$L.equals(column))", columnsClassName, upperCase)
              .addStatement("return $N.$L()", "entity", timestampGetter);

          setColumnTimestampBuilder
              .beginControlFlow("if ($T.$L.equals(column))", columnsClassName, upperCase)
              .addStatement("$N.$L($N)", "entity", timestampSetter, "timestamp");
        } else {
          getColumnTimestampBuilder
              .nextControlFlow("else if ($T.$L.equals(column))", columnsClassName, upperCase)
              .addStatement("return $N.$L()", "entity", timestampGetter);

          setColumnTimestampBuilder
              .nextControlFlow("else if ($T.$L.equals(column))", columnsClassName, upperCase)
              .addStatement("$N.$L($N)", "entity", timestampSetter, "timestamp");
        }
      }

      entityBuilder.addMethod(setterBuilder.build());

      final String columnFamily =
          Optional.of(column.family())
              .filter(family -> !family.isEmpty())
              .orElseThrow(
                  () ->
                      new ElementProcessingException(
                          "@Column family must be defined", columnElement));

      final String columnQualifier =
          Optional.of(column.qualifier())
              .filter(qualifier -> !qualifier.isEmpty())
              .orElse(lowerCamelCase);

      if (!columnHashes.add(Objects.hash(columnFamily, columnQualifier))) {
        throw new ElementProcessingException("Duplicate @Column definition", columnElement);
      }

      final Class<?> equalsType;

      if (TypeKind.ARRAY.equals(kind)) {
        equalsType = Arrays.class;
        arrayFields.add(lowerCamelCase);
      } else {
        equalsType = Objects.class;
        objectFields.add(lowerCamelCase);
      }

      final String equalsPrefix;

      if (!startedControlFlow.getAndSet(true)) {
        getColumnValueBuilder
            .beginControlFlow("if ($T.$L.equals(column))", columnsClassName, upperCase)
            .addStatement("return $N.$L()", "entity", getter);

        setColumnValueBuilder
            .beginControlFlow("if ($T.$L.equals(column))", columnsClassName, upperCase)
            .addStatement("$N.$L(($T) $N)", "entity", setter, typeName, "value");

        equalsPrefix = "";
        toStringReturnBuilder.add("\n+ \"");
      } else {
        getColumnValueBuilder
            .nextControlFlow("else if ($T.$L.equals(column))", columnsClassName, upperCase)
            .addStatement("return $N.$L()", "entity", getter);

        setColumnValueBuilder
            .nextControlFlow("else if ($T.$L.equals(column))", columnsClassName, upperCase)
            .addStatement("$N.$L(($T) $N)", "entity", setter, typeName, "value");

        equalsPrefix = "\n" + "&& ";
        toStringReturnBuilder.add("\n+ \", ");
      }

      equalsReturnBuilder.add(
          equalsPrefix + "$T.equals($N, $N.$N)",
          equalsType,
          lowerCamelCase,
          "that",
          lowerCamelCase);

      if (TypeKind.ARRAY.equals(kind)) {
        toStringReturnBuilder.add(
            "$L=\" + $T.toString($N)", lowerCamelCase, Arrays.class, lowerCamelCase);
      } else if (TypeName.get(String.class).equals(typeName)) {
        toStringReturnBuilder.add("$L='\" + $N + '\\''", lowerCamelCase, lowerCamelCase);
      } else {
        toStringReturnBuilder.add("$L=\" + $N", lowerCamelCase, lowerCamelCase);
      }

      columnsBuilder.addEnumConstant(
          upperCase,
          TypeSpec.anonymousClassBuilder(
                  "$S, $S, new $T<$T>() { }, $L",
                  columnFamily,
                  columnQualifier,
                  TypeReference.class,
                  typeName,
                  column.versioned())
              .build());
    }

    getColumnValueBuilder.nextControlFlow("else");
    getColumnValueBuilder.addStatement(
        "throw new $T($S)", IllegalArgumentException.class, "Unrecognized column");
    getColumnValueBuilder.endControlFlow();

    setColumnValueBuilder.nextControlFlow("else");
    setColumnValueBuilder.addStatement(
        "throw new $T($S)", IllegalArgumentException.class, "Unrecognized column");
    setColumnValueBuilder.endControlFlow();

    if (startedTimestampControlFlow.get()) {
      getColumnTimestampBuilder.nextControlFlow("else");
      setColumnTimestampBuilder.nextControlFlow("else");
    }

    getColumnTimestampBuilder.addStatement(
        "throw new $T($S)", IllegalArgumentException.class, "Invalid column");
    setColumnTimestampBuilder.addStatement(
        "throw new $T($S)", IllegalArgumentException.class, "Invalid column");

    if (startedTimestampControlFlow.get()) {
      getColumnTimestampBuilder.endControlFlow();
      setColumnTimestampBuilder.endControlFlow();
    }

    entityDelegateBuilder.addMethod(getColumnValueBuilder.build());
    entityDelegateBuilder.addMethod(setColumnValueBuilder.build());
    entityDelegateBuilder.addMethod(getColumnTimestampBuilder.build());
    entityDelegateBuilder.addMethod(setColumnTimestampBuilder.build());

    entityConfigurationBuilder.addMethod(
        MethodSpec.methodBuilder("getDelegateForEntity")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(EntityConfiguration.EntityDelegate.class), entityClassName))
            .addParameter(entityClassName, "entity", Modifier.FINAL)
            .addStatement("return new $T($N)", delegateClassName, "entity")
            .build());

    columnsBuilder.addField(String.class, "family", Modifier.PRIVATE, Modifier.FINAL);
    columnsBuilder.addField(String.class, "qualifier", Modifier.PRIVATE, Modifier.FINAL);
    columnsBuilder.addField(
        ParameterizedTypeName.get(
            ClassName.get(TypeReference.class), WildcardTypeName.subtypeOf(Object.class)),
        "typeReference",
        Modifier.PRIVATE,
        Modifier.FINAL);
    columnsBuilder.addField(TypeName.BOOLEAN, "isVersioned", Modifier.PRIVATE, Modifier.FINAL);

    columnsBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addParameter(String.class, "family")
            .addParameter(String.class, "qualifier")
            .addParameter(
                ParameterizedTypeName.get(
                    ClassName.get(TypeReference.class), WildcardTypeName.subtypeOf(Object.class)),
                "typeReference")
            .addParameter(TypeName.BOOLEAN, "isVersioned")
            .addStatement("this.$N = $N", "family", "family")
            .addStatement("this.$N = $N", "qualifier", "qualifier")
            .addStatement("this.$N = $N", "typeReference", "typeReference")
            .addStatement("this.$N = $N", "isVersioned", "isVersioned")
            .build());

    columnsBuilder.addMethod(
        MethodSpec.methodBuilder("getFamily")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $N", "family")
            .build());

    columnsBuilder.addMethod(
        MethodSpec.methodBuilder("getQualifier")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $N", "qualifier")
            .build());

    columnsBuilder.addMethod(
        MethodSpec.methodBuilder("getTypeReference")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(TypeReference.class), WildcardTypeName.subtypeOf(Object.class)))
            .addStatement("return $N", "typeReference")
            .build());

    columnsBuilder.addMethod(
        MethodSpec.methodBuilder("isVersioned")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addStatement("return $N", "isVersioned")
            .build());

    equalsBuilder.addStatement(equalsReturnBuilder.build());
    entityBuilder.addMethod(equalsBuilder.build());

    final String objectFieldsCsv = String.join(", ", objectFields);

    final CodeBlock objectsHashCodeBlock =
        CodeBlock.builder().add("$T.hash($L)", Objects.class, objectFieldsCsv).build();

    if (arrayFields.isEmpty()) {
      hashCodeBuilder.addStatement("return $L", objectsHashCodeBlock);
    } else {
      final int arrayFieldStartIndex;
      final boolean shouldReturn;

      if (objectFields.isEmpty()) {
        final CodeBlock firstArrayHashCodeBlock =
            CodeBlock.builder().add("$T.hashCode($N)", Arrays.class, arrayFields.get(0)).build();

        if (arrayFields.size() > 1) {
          hashCodeBuilder.addStatement("int result = $L", firstArrayHashCodeBlock);
          shouldReturn = true;
        } else {
          hashCodeBuilder.addStatement("return $L", firstArrayHashCodeBlock);
          shouldReturn = false;
        }

        arrayFieldStartIndex = 1;
      } else {
        hashCodeBuilder.addStatement("int result = $L", objectsHashCodeBlock);
        arrayFieldStartIndex = 0;
        shouldReturn = true;
      }

      IntStream.range(arrayFieldStartIndex, arrayFields.size())
          .mapToObj(arrayFields::get)
          .forEachOrdered(
              arrayField ->
                  hashCodeBuilder.addStatement(
                      "$N = 31 * $N + $T.hashCode($N)",
                      "result",
                      "result",
                      Arrays.class,
                      arrayField));

      if (shouldReturn) {
        hashCodeBuilder.addStatement("return $N", "result");
      }
    }

    entityBuilder.addMethod(hashCodeBuilder.build());

    toStringReturnBuilder.add("\n+ '}'");
    toStringBuilder.addStatement(toStringReturnBuilder.build());
    entityBuilder.addMethod(toStringBuilder.build());

    final Entity annotatedEntity = entityElement.getAnnotation(Entity.class);
    final List<KeyComponent> keyComponents = Arrays.asList(annotatedEntity.keyComponents());

    if (keyComponents.isEmpty()) {
      throw new ElementProcessingException(
          "@Entity types require at least 1 @KeyComponent", entityElement);
    }

    final ClassName keyBuilderClassName = entityClassName.nestedClass(entityName + "KeyBuilder");
    final TypeSpec.Builder keyBuilderBuilder =
        TypeSpec.classBuilder(keyBuilderClassName)
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addComment("Prevent external instantiation")
                    .build());

    final Map<String, Object> namedBuildParameters = new HashMap<>();

    final TypeName parameterizedKeyBuilderName =
        ParameterizedTypeName.get(ClassName.get(KeyBuilder.class), entityClassName);
    final TypeName parameterizedKeyName =
        ParameterizedTypeName.get(ClassName.get(Key.class), entityClassName);

    final Map<Integer, KeyComponent> constantComponents =
        IntStream.range(0, keyComponents.size())
            .filter(
                i ->
                    Optional.of(keyComponents.get(i))
                        .map(KeyComponent::constant)
                        .filter(s -> !s.isEmpty())
                        .isPresent())
            .boxed()
            .collect(Collectors.toMap(Function.identity(), keyComponents::get));

    constantComponents.forEach(
        (i, keyComponent) -> {
          final String name = String.join("_", "COMPONENT", String.valueOf(i));

          keyBuilderBuilder.addField(
              FieldSpec.builder(
                      String.class, name, Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                  .initializer("$S", keyComponent.constant())
                  .build());

          namedBuildParameters.put("keyComponent" + i, name);
        });

    final AtomicInteger stepCounter = new AtomicInteger(0);

    final Map<Integer, KeyComponent> dynamicComponents =
        IntStream.range(0, keyComponents.size())
            .filter(i -> !constantComponents.containsKey(i))
            .mapToObj(
                i -> {
                  final int step = stepCounter.getAndIncrement();
                  final KeyComponent keyComponent = keyComponents.get(i);

                  namedBuildParameters.put("keyComponent" + i, keyComponent.name());

                  return new AbstractMap.SimpleImmutableEntry<>(step, keyComponent);
                })
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (u, v) -> {
                      // Impossible
                      throw new IllegalStateException("Duplicate key: " + u);
                    },
                    LinkedHashMap::new // Preserves order of declared components
                    ));

    final Map<Integer, ClassName> stepClassNames =
        dynamicComponents.entrySet().stream()
            .map(
                entry -> {
                  final String lowerCamelCase = entry.getValue().name();
                  final String upperCamelCase =
                      CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, lowerCamelCase);

                  final ClassName className =
                      entityClassName.nestedClass("Requires" + upperCamelCase);

                  return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), className);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    dynamicComponents.forEach(
        (step, keyComponent) -> {
          final TypeMirror typeMirror =
              getTypeMirrorFromAnnotation(keyComponent, KeyComponent::type);

          final ClassName className = stepClassNames.get(step);

          final TypeName nextTypeName =
              Optional.ofNullable(stepClassNames.get(step + 1))
                  .map(TypeName.class::cast)
                  .orElse(parameterizedKeyBuilderName);

          final String lowerCamelCase = keyComponent.name();

          entityBuilder.addType(
              TypeSpec.interfaceBuilder(className)
                  .addModifiers(Modifier.PUBLIC)
                  .addMethod(
                      MethodSpec.methodBuilder(lowerCamelCase)
                          .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                          .returns(nextTypeName)
                          .addParameter(TypeName.get(typeMirror), lowerCamelCase, Modifier.FINAL)
                          .build())
                  .build());

          keyBuilderBuilder
              .addSuperinterface(className)
              .addField(TypeName.get(typeMirror), lowerCamelCase, Modifier.PRIVATE)
              .addMethod(
                  MethodSpec.methodBuilder(lowerCamelCase)
                      .addAnnotation(Override.class)
                      .addModifiers(Modifier.PUBLIC)
                      .returns(nextTypeName)
                      .addParameter(TypeName.get(typeMirror), lowerCamelCase, Modifier.FINAL)
                      .addStatement("this.$N = $N", lowerCamelCase, lowerCamelCase)
                      .addStatement("return this")
                      .build());
        });

    keyBuilderBuilder.addSuperinterface(parameterizedKeyBuilderName);

    // These names are getting ridiculous
    final MethodSpec.Builder keyBuilderBuildBuilder =
        MethodSpec.methodBuilder("build")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(parameterizedKeyName);

    final String streamOfComponents =
        IntStream.range(0, keyComponents.size())
            .boxed()
            .map(i -> "$keyComponent" + i + ":N")
            .collect(Collectors.joining(", "));

    final String keyStringName = "keyString";

    namedBuildParameters.put("string", String.class);
    namedBuildParameters.put("var", keyStringName);
    namedBuildParameters.put("stream", Stream.class);
    namedBuildParameters.put("objects", Objects.class);
    namedBuildParameters.put("object", Object.class);
    namedBuildParameters.put("collectors", Collectors.class);
    namedBuildParameters.put("delimiter", annotatedEntity.keyDelimiter());
    namedBuildParameters.put("indent", INDENT);

    keyBuilderBuildBuilder
        .addNamedCode(
            "final $string:T $var:L = $stream:T.of("
                + streamOfComponents
                + ")\n"
                + "$indent:L$indent:L.peek($objects:T::requireNonNull)\n"
                + "$indent:L$indent:L.map($object:T::toString)\n"
                + "$indent:L$indent:L.collect($collectors:T.joining($delimiter:S));\n",
            namedBuildParameters)
        .addStatement("return new $T<>($N)", StringKey.class, keyStringName);

    keyBuilderBuilder.addMethod(keyBuilderBuildBuilder.build());

    final TypeName keyBuilderTypeName =
        Optional.ofNullable(stepClassNames.get(0))
            .map(TypeName.class::cast)
            .orElse(parameterizedKeyBuilderName);

    entityBuilder.addMethod(
        MethodSpec.methodBuilder("keyBuilder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(keyBuilderTypeName)
            .addStatement("return new $T()", keyBuilderClassName)
            .build());

    entityBuilder.addStaticBlock(
        CodeBlock.builder()
            .addStatement(
                "register($T.$N, $T.class)", configurationClassName, "INSTANCE", entityClassName)
            .build());

    entityBuilder.addType(keyBuilderBuilder.build());
    entityBuilder.addType(columnsBuilder.build());
    entityBuilder.addType(entityConfigurationBuilder.build());
    entityBuilder.addType(entityDelegateBuilder.build());

    return JavaFile.builder(packageName, entityBuilder.build()).indent(INDENT).build();
  }

  /**
   * This is the stupidest thing I've ever seen, but I get it.
   *
   * <p>"The annotation returned by this method could contain an element whose value is of type
   * {@link Class}. This value cannot be returned directly: information necessary to locate and load
   * a class (such as the class loader to use) is not available, and the class might not be loadable
   * at all. Attempting to read a {@link Class} object by invoking the relevant method on the
   * returned annotation will result in a {@link MirroredTypeException}, from which the
   * corresponding {@link TypeMirror} may be extracted."
   *
   * @see <a
   *     href="https://docs.oracle.com/javase/8/docs/api/javax/lang/model/element/Element.html#getAnnotation-java.lang.Class-">https://docs.oracle.com/javase/8/docs/api/javax/lang/model/element/Element.html#getAnnotation-java.lang.Class-</a>
   */
  private static <T extends Annotation> TypeMirror getTypeMirrorFromAnnotation(
      final T annotation, final Function<T, Class<?>> classFunction) {
    try {
      classFunction.apply(annotation);
      throw new IllegalStateException(
          "Annotation function did not throw the expected MirroredTypeException");
    } catch (final MirroredTypeException e) {
      return e.getTypeMirror();
    }
  }
}
