package com.bettercloud.bigtable.orm.process;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Test;

import javax.tools.JavaFileObject;

import static com.google.testing.compile.Compiler.javac;
import static org.junit.Assert.assertEquals;

public class EntityAnnotationProcessorTest {

    private static final String PACKAGE_NAME = "com.bettercloud.bigtable.orm.test";

    @Test
    public void testEntityProcessorSucceedsWithValidConfiguration() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("ValidConfiguration.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.SUCCESS, compilation.status());

        final JavaFileObject generated = compilation.generatedSourceFile(PACKAGE_NAME + ".MyEntity")
                .orElseThrow(IllegalStateException::new);

        final Compilation generatedCompilation = javac().compile(generated);

        assertEquals(Compilation.Status.SUCCESS, generatedCompilation.status());
    }

    @Test
    public void testEntityProcessorSucceedsWithValidArrayOnlyConfiguration() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("ValidArrayOnlyConfiguration.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.SUCCESS, compilation.status());

        final JavaFileObject generated = compilation.generatedSourceFile(PACKAGE_NAME + ".MyEntity")
                .orElseThrow(IllegalStateException::new);

        final Compilation generatedCompilation = javac().compile(generated);

        assertEquals(Compilation.Status.SUCCESS, generatedCompilation.status());
    }

    @Test
    public void testEntityProcessorSucceedsWithValidSingleArrayConfiguration() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("ValidSingleArrayConfiguration.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.SUCCESS, compilation.status());

        final JavaFileObject generated = compilation.generatedSourceFile(PACKAGE_NAME + ".MyEntity")
                .orElseThrow(IllegalStateException::new);

        final Compilation generatedCompilation = javac().compile(generated);

        assertEquals(Compilation.Status.SUCCESS, generatedCompilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityIsNotPrivate() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("NonPrivateEntity.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityIsStatic() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("StaticEntity.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenTableIsPublic() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("PublicTable.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityHasNoParent() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("NoParent.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityParentHasNoTableAnnotation() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("NoTable.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityParentHasEmptyTableAnnotation() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("EmptyTable.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityHasNoColumns() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("NoColumns.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityHasDuplicateColumns() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("DuplicateColumn.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityColumnIsStatic() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("StaticColumn.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityColumnIsFinal() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("FinalColumn.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityContainsColumnWithEmptyColumnFamily() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("EmptyColumnFamily.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }

    @Test
    public void testEntityProcessorFailsWhenEntityHasNoKeyComponents() {
        final JavaFileObject javaFileObject = JavaFileObjects.forResource("NoKeyComponents.java");

        final Compilation compilation = javac().withProcessors(new EntityAnnotationProcessor()).compile(javaFileObject);

        assertEquals(Compilation.Status.FAILURE, compilation.status());
    }
}
