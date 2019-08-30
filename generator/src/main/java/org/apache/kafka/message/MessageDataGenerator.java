/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.message;

import java.io.Writer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates Kafka MessageData classes.
 */
public final class MessageDataGenerator {
    private final StructRegistry structRegistry;
    private final HeaderGenerator headerGenerator;
    private final SchemaGenerator schemaGenerator;
    private final CodeBuffer buffer;
    private Versions flexibleVersions;

    MessageDataGenerator() {
        this.structRegistry = new StructRegistry();
        this.headerGenerator = new HeaderGenerator();
        this.schemaGenerator = new SchemaGenerator(headerGenerator, structRegistry);
        this.buffer = new CodeBuffer();
    }

    void generate(MessageSpec message) throws Exception {
        if (message.struct().versions().contains(Short.MAX_VALUE)) {
            throw new RuntimeException("Message " + message.name() + " does " +
                "not specify a maximum version.");
        }
        structRegistry.register(message);
        schemaGenerator.generateSchemas(message, message.flexibleVersions());
        flexibleVersions = message.flexibleVersions();
        generateClass(Optional.of(message),
            message.name() + "Data",
            message.struct(),
            message.struct().versions());
        headerGenerator.generate();
    }

    void write(Writer writer) throws Exception {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private void generateClass(Optional<MessageSpec> topLevelMessageSpec,
                               String className,
                               StructSpec struct,
                               Versions parentVersions) throws Exception {
        buffer.printf("%n");
        boolean isTopLevel = topLevelMessageSpec.isPresent();
        boolean isSetElement = struct.hasKeys(); // Check if the class is inside a set.
        if (isTopLevel && isSetElement) {
            throw new RuntimeException("Cannot set mapKey on top level fields.");
        }
        generateClassHeader(className, isTopLevel, isSetElement);
        buffer.incrementIndent();
        generateFieldDeclarations(struct, isSetElement);
        buffer.printf("%n");
        schemaGenerator.writeSchema(className, buffer);
        generateClassConstructors(className, struct);
        buffer.printf("%n");
        if (isTopLevel) {
            generateShortAccessor("apiKey", topLevelMessageSpec.get().apiKey().orElse((short) -1));
        }
        buffer.printf("%n");
        generateShortAccessor("lowestSupportedVersion", parentVersions.lowest());
        buffer.printf("%n");
        generateShortAccessor("highestSupportedVersion", parentVersions.highest());
        buffer.printf("%n");
        generateClassReader(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassWriter(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassFromStruct(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassToStruct(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassSize(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassEquals(className, struct, isSetElement);
        buffer.printf("%n");
        generateClassHashCode(struct, isSetElement);
        buffer.printf("%n");
        generateClassToString(className, struct);
        generateFieldAccessors(struct, isSetElement);
        buffer.printf("%n");
        generateUnknownTaggedFieldsAccessor(struct);
        generateFieldMutators(struct, className, isSetElement);

        if (!isTopLevel) {
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        generateSubclasses(className, struct, parentVersions, isSetElement);
        if (isTopLevel) {
            for (Iterator<StructSpec> iter = structRegistry.commonStructs(); iter.hasNext(); ) {
                StructSpec commonStruct = iter.next();
                generateClass(Optional.empty(),
                        commonStruct.name(),
                        commonStruct,
                        commonStruct.versions());
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateClassHeader(String className, boolean isTopLevel,
                                     boolean isSetElement) {
        Set<String> implementedInterfaces = new HashSet<>();
        if (isTopLevel) {
            implementedInterfaces.add("ApiMessage");
            headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);
        } else {
            implementedInterfaces.add("Message");
            headerGenerator.addImport(MessageGenerator.MESSAGE_CLASS);
        }
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
            implementedInterfaces.add("ImplicitLinkedHashMultiCollection.Element");
        }
        Set<String> classModifiers = new HashSet<>();
        classModifiers.add("public");
        if (!isTopLevel) {
            classModifiers.add("static");
        }
        buffer.printf("%s class %s implements %s {%n",
            String.join(" ", classModifiers),
            className,
            String.join(", ", implementedInterfaces));
    }

    private void generateSubclasses(String className, StructSpec struct,
            Versions parentVersions, boolean isSetElement) throws Exception {
        for (FieldSpec field : struct.fields()) {
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                if (!structRegistry.commonStructNames().contains(arrayType.elementName())) {
                    generateClass(Optional.empty(),
                            arrayType.elementType().toString(),
                            structRegistry.findStruct(field),
                            parentVersions.intersect(struct.versions()));
                }
            }
        }
        if (isSetElement) {
            generateHashSet(className, struct);
        }
    }

    private void generateHashSet(String className, StructSpec struct) {
        buffer.printf("%n");
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("public static class %s extends ImplicitLinkedHashMultiCollection<%s> {%n",
            collectionType(className), className);
        buffer.incrementIndent();
        generateHashSetZeroArgConstructor(className);
        generateHashSetSizeArgConstructor(className);
        generateHashSetIteratorConstructor(className);
        generateHashSetFindMethod(className, struct);
        generateHashSetFindAllMethod(className, struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetZeroArgConstructor(String className) {
        buffer.printf("public %s() {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetSizeArgConstructor(String className) {
        buffer.printf("public %s(int expectedNumElements) {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super(expectedNumElements);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetIteratorConstructor(String className) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS);
        buffer.printf("public %s(Iterator<%s> iterator) {%n", collectionType(className), className);
        buffer.incrementIndent();
        buffer.printf("super(iterator);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetFindMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public %s find(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return find(key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetFindAllMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public List<%s> findAll(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return findAll(key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateKeyElement(String className, StructSpec struct) {
        buffer.printf("%s key = new %s();%n", className, className);
        for (FieldSpec field : struct.fields()) {
            if (field.mapKey()) {
                buffer.printf("key.set%s(%s);%n",
                    field.capitalizedCamelCaseName(),
                    field.camelCaseName());
            }
        }
    }

    private String commaSeparatedHashSetFieldAndTypes(StructSpec struct) {
        return struct.fields().stream().
            filter(f -> f.mapKey()).
            map(f -> String.format("%s %s", fieldConcreteJavaType(f), f.camelCaseName())).
            collect(Collectors.joining(", "));
    }

    private void generateFieldDeclarations(StructSpec struct, boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldDeclaration(field);
        }
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS);
        buffer.printf("private List<RawTaggedField> _unknownTaggedFields;%n");
        if (isSetElement) {
            buffer.printf("private int next;%n");
            buffer.printf("private int prev;%n");
        }
    }

    private void generateFieldDeclaration(FieldSpec field) {
        buffer.printf("private %s %s;%n",
            fieldAbstractJavaType(field), field.camelCaseName());
    }

    private void generateFieldAccessors(StructSpec struct, boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldAccessor(field);
        }
        if (isSetElement) {
            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateAccessor("int", "next", "next");

            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateAccessor("int", "prev", "prev");
        }
    }

    private void generateUnknownTaggedFieldsAccessor(StructSpec struct) {
        buffer.printf("@Override%n");
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS);
        buffer.printf("public List<RawTaggedField> unknownTaggedFields() {%n");
        buffer.incrementIndent();
        // Optimize _unknownTaggedFields by not creating a new list object
        // unless we need it.
        buffer.printf("if (_unknownTaggedFields == null) {%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
        buffer.printf("_unknownTaggedFields = new ArrayList<>(0);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("return _unknownTaggedFields;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");

    }

    private void generateFieldMutators(StructSpec struct, String className,
                                       boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldMutator(className, field);
        }
        if (isSetElement) {
            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateSetter("int", "setNext", "next");

            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateSetter("int", "setPrev", "prev");
        }
    }

    private static String collectionType(String baseType) {
        return baseType + "Collection";
    }

    private String fieldAbstractJavaType(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            return "boolean";
        } else if (field.type() instanceof FieldType.Int8FieldType) {
            return "byte";
        } else if (field.type() instanceof FieldType.Int16FieldType) {
            return "short";
        } else if (field.type() instanceof FieldType.Int32FieldType) {
            return "int";
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            return "long";
        } else if (field.type().isString()) {
            return "String";
        } else if (field.type().isBytes()) {
            return "byte[]";
        } else if (field.type().isStruct()) {
            return MessageGenerator.capitalizeFirst(field.typeString());
        } else if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (structRegistry.isStructArrayWithKeys(field)) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.LIST_CLASS);
                return "List<" + getBoxedJavaType(arrayType.elementType()) + ">";
            }
        } else {
            throw new RuntimeException("Unknown field type " + field.type());
        }
    }

    private String fieldConcreteJavaType(FieldSpec field) {
        if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (structRegistry.isStructArrayWithKeys(field)) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                return "ArrayList<" + getBoxedJavaType(arrayType.elementType()) + ">";
            }
        } else {
            return fieldAbstractJavaType(field);
        }
    }

    private void generateClassConstructors(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("public %s(Readable readable, short version) {%n", className);
        buffer.incrementIndent();
        initializeArrayDefaults(struct);
        buffer.printf("read(readable, version);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("public %s(Struct struct, short version) {%n", className);
        buffer.incrementIndent();
        initializeArrayDefaults(struct);
        buffer.printf("fromStruct(struct, version);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
        buffer.printf("public %s() {%n", className);
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(), fieldDefault(field));
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void initializeArrayDefaults(StructSpec struct) {
        for (FieldSpec field : struct.fields()) {
            if (field.type().isArray()) {
                buffer.printf("this.%s = %s;%n",
                    field.camelCaseName(), fieldDefault(field));
            }
        }
    }

    private void generateShortAccessor(String name, short val) {
        buffer.printf("@Override%n");
        buffer.printf("public short %s() {%n", name);
        buffer.incrementIndent();
        buffer.printf("return %d;%n", val);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassReader(String className, StructSpec struct,
                                     Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void read(Readable readable, short version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember((__) -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                    "version \" + version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            if (!field.taggedVersions().intersect(flexibleVersions).equals(field.taggedVersions())) {
                throw new RuntimeException("Field " + field.name() + " specifies tagged " +
                    "versions " + field.taggedVersions() + " that are not a subset of the " +
                    "flexible versions " + flexibleVersions);
            }
            Versions mandatoryVersions = field.versions().subtract(field.taggedVersions());
            VersionConditional.forVersions(mandatoryVersions, curVersions).
                alwaysEmitBlockScope(field.type().isVariableLength()).
                ifNotMember((__) -> {
                    // If the field is not present, or is tagged, set it to its default here.
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
                }).
                ifMember((presentAndUntaggedVersions) -> {
                    if (field.type().isVariableLength()) {
                        generateVariableLengthReader(field.camelCaseName(),
                            field.type(),
                            presentAndUntaggedVersions,
                            field.nullableVersions(),
                            String.format("this.%s = ", field.camelCaseName()),
                            String.format(";%n"),
                            structRegistry.isStructArrayWithKeys(field));
                    } else {
                        buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                            primitiveReadExpression(field.type()));
                    }
                }).
                generate(buffer);
        }
        buffer.printf("this._unknownTaggedFields = null;%n");
        VersionConditional.forVersions(flexibleVersions, curVersions).
            ifMember((curFlexibleVersions) -> {
                buffer.printf("int _numTaggedFields = readable.readUnsignedVarint();%n");
                buffer.printf("for (int _i = 0; _i < _numTaggedFields; _i++) {%n");
                buffer.incrementIndent();
                buffer.printf("int _tag = readable.readUnsignedVarint();%n");
                buffer.printf("int _size = readable.readUnsignedVarint();%n");
                buffer.printf("switch (_tag) {%n");
                buffer.incrementIndent();
                for (FieldSpec field : struct.fields()) {
                    Versions validTaggedVersions = field.versions().intersect(field.taggedVersions());
                    if (!validTaggedVersions.empty()) {
                        if (!field.tag().isPresent()) {
                            throw new RuntimeException("Field " + field.name() + " has tagged versions, but no tag.");
                        }
                        buffer.printf("case %d: {%n", field.tag().get());
                        buffer.incrementIndent();
                        VersionConditional.forVersions(validTaggedVersions, curFlexibleVersions).
                            ifMember((presentAndTaggedVersions) -> {
                                if (field.type().isVariableLength()) {
                                    generateVariableLengthReader(field.camelCaseName(),
                                        field.type(),
                                        presentAndTaggedVersions,
                                        field.nullableVersions(),
                                        String.format("this.%s = ", field.camelCaseName()),
                                        String.format(";%n"),
                                        structRegistry.isStructArrayWithKeys(field));
                                } else {
                                    buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                                        primitiveReadExpression(field.type()));
                                }
                                buffer.printf("break;%n");
                            }).
                            ifNotMember((__) -> {
                                buffer.printf("throw new RuntimeException(\"Tag %d is not " +
                                    "valid for version \" + version);%n", field.tag().get());
                            }).
                            generate(buffer);
                        buffer.decrementIndent();
                        buffer.printf("}%n");
                    }
                }
                buffer.printf("default:%n");
                buffer.incrementIndent();
                buffer.printf("this._unknownTaggedFields = readable.readRawTaggedField(this._unknownTaggedFields, _tag, _size);%n");
                buffer.printf("break;%n");
                buffer.decrementIndent();
                buffer.decrementIndent();
                buffer.printf("}%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            generate(buffer);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "readable.readByte() != 0";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "readable.readByte()";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "readable.readShort()";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "readable.readInt()";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "readable.readLong()";
        } else if (type.isStruct()) {
            return String.format("new %s(readable, version)", type.toString());
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateVariableLengthReader(String name,
                                              FieldType type,
                                              Versions versions,
                                              Versions nullableVersions,
                                              String assignmentPrefix,
                                              String assignmentSuffix,
                                              boolean isStructArrayWithKeys) {
        String lengthVar = type.isArray() ? "arrayLength" : "length";
        buffer.printf("int %s;%n", lengthVar);
        VersionConditional.forVersions(flexibleVersions, versions).
            ifMember((__) -> {
                buffer.printf("%s = readable.readUnsignedVarint() - 1;%n", lengthVar);
            }).
            ifNotMember((__) -> {
                if (type.isString()) {
                    buffer.printf("%s = readable.readShort();%n", lengthVar);
                } else if (type.isBytes() || type.isArray()) {
                    buffer.printf("%s = readable.readInt();%n", lengthVar);
                } else {
                    throw new RuntimeException("Can't handle variable length type " + type);
                }
            }).
            generate(buffer);
        buffer.printf("if (%s < 0) {%n", lengthVar);
        buffer.incrementIndent();
        VersionConditional.forVersions(nullableVersions, versions).
            ifNotMember((__) -> {
                buffer.printf("throw new RuntimeException(\"non-nullable field %s " +
                    "was serialized as null\");%n", name);
            }).
            ifMember((__) -> {
                buffer.printf("%snull%s", assignmentPrefix, assignmentSuffix);
            }).
            generate(buffer);
        buffer.decrementIndent();
        if (type.isString()) {
            buffer.printf("} else if (%s > 0x7fff) {%n", lengthVar);
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"string field %s " +
                "had invalid length \" + %s);%n", name, lengthVar);
            buffer.decrementIndent();
        }
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        if (type.isString()) {
            buffer.printf("%sreadable.readString(%s)%s",
                assignmentPrefix, lengthVar, assignmentSuffix);
        } else if (type.isBytes()) {
            buffer.printf("byte[] newBytes = new byte[%s];%n", lengthVar);
            buffer.printf("readable.readArray(newBytes);%n");
            buffer.printf("%snewBytes%s", assignmentPrefix, assignmentSuffix);
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            if (isStructArrayWithKeys) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                buffer.printf("%s newCollection = new %s(%s);%n",
                    collectionType(arrayType.elementType().toString()),
                        collectionType(arrayType.elementType().toString()), lengthVar);
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                buffer.printf("ArrayList<%s> newCollection = new ArrayList<%s>(%s);%n",
                    getBoxedJavaType(arrayType.elementType()),
                        getBoxedJavaType(arrayType.elementType()), lengthVar);
            }
            buffer.printf("for (int i = 0; i < %s; i++) {%n", lengthVar);
            buffer.incrementIndent();
            if (arrayType.elementType().isArray()) {
                throw new RuntimeException("Nested arrays are not supported.  " +
                    "Use an array of structures containing another array.");
            } else if (arrayType.elementType().isBytes() || arrayType.elementType().isString()) {
                generateVariableLengthReader(name + " element",
                    arrayType.elementType(),
                    versions,
                    Versions.NONE,
                    "newCollection.add(",
                    String.format(");%n"),
                    false);
            } else {
                buffer.printf("newCollection.add(%s);%n",
                    primitiveReadExpression(arrayType.elementType()));
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%snewCollection%s", assignmentPrefix, assignmentSuffix);
        } else {
            throw new RuntimeException("Can't handle variable length type " + type);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassFromStruct(String className, StructSpec struct,
                                         Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void fromStruct(Struct struct, short version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember((__) -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                    "version \" + version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            VersionConditional.forVersions(field.versions(), curVersions).
                alwaysEmitBlockScope(field.type().isArray()).
                ifNotMember((__) -> {
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
                }).
                ifMember((presentVersions) -> {
                    generateFieldFromStruct(field, presentVersions);
                }).
                generate(buffer);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldFromStruct(FieldSpec field, Versions presentVersions) {
        VersionConditional.forVersions(field.taggedVersions(), presentVersions).
            ifNotMember((presentAndUntaggedVersions) -> {
                if (field.type().isArray()) {
                    generateArrayFromStruct(field, presentAndUntaggedVersions);
                } else {
                    buffer.printf("this.%s = %s;%n",
                        field.camelCaseName(),
                        readFieldFromStruct(field.type(), field.snakeCaseName()));
                }
            }).
            generate(buffer);
    }

    private void generateArrayFromStruct(FieldSpec field, Versions versions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("Object[] nestedObjects = struct.getArray(\"%s\");%n",
            field.snakeCaseName());
        boolean maybeNull = false;
        if (!versions.intersect(field.nullableVersions()).empty()) {
            maybeNull = true;
            buffer.printf("if (nestedObjects == null) {%n", field.camelCaseName());
            buffer.incrementIndent();
            buffer.printf("this.%s = null;%n", field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
        }
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
        FieldType elementType = arrayType.elementType();
        buffer.printf("this.%s = new %s(nestedObjects.length);%n",
            field.camelCaseName(), fieldConcreteJavaType(field));
        buffer.printf("for (Object nestedObject : nestedObjects) {%n");
        buffer.incrementIndent();
        if (elementType.isStruct()) {
            buffer.printf("this.%s.add(new %s((Struct) nestedObject, version));%n",
                field.camelCaseName(), elementType.toString());
        } else {
            buffer.printf("this.%s.add((%s) nestedObject);%n",
                field.camelCaseName(), getBoxedJavaType(elementType));
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
        if (maybeNull) {
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private String getBoxedJavaType(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "Boolean";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "Byte";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "Short";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "Integer";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "Long";
        } else if (type.isString()) {
            return "String";
        } else if (type.isStruct()) {
            return type.toString();
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String readFieldFromStruct(FieldType type, String name) {
        if (type instanceof FieldType.BoolFieldType) {
            return String.format("struct.getBoolean(\"%s\")", name);
        } else if (type instanceof FieldType.Int8FieldType) {
            return String.format("struct.getByte(\"%s\")", name);
        } else if (type instanceof FieldType.Int16FieldType) {
            return String.format("struct.getShort(\"%s\")", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("struct.getInt(\"%s\")", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("struct.getLong(\"%s\")", name);
        } else if (type.isString()) {
            return String.format("struct.getString(\"%s\")", name);
        } else if (type.isBytes()) {
            return String.format("struct.getByteArray(\"%s\")", name);
        } else if (type.isStruct()) {
            return String.format("new %s(struct, version)", type.toString());
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateClassWriter(String className, StructSpec struct,
            Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.WRITABLE_CLASS);
        headerGenerator.addImport(MessageGenerator.OBJECT_SIZE_CACHE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void write(Writable writable, ObjectSizeCache sizeCache, short version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember((__) -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                    "version \" + version + \" of %s\");%n", className);
            }).
            generate(buffer);
        buffer.printf("int _numTaggedFields = 0;%n");
        Versions curVersions = parentVersions.intersect(struct.versions());
        TreeMap<Integer, FieldSpec> taggedFields = new TreeMap<>();
        for (FieldSpec field : struct.fields()) {
            VersionConditional cond = VersionConditional.forVersions(field.versions(), curVersions).
                ifMember((presentVersions) -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember((presentAndUntaggedVersions) -> {
                            if (field.type().isVariableLength()) {
                                generateVariableLengthWriter(field.camelCaseName(),
                                    field.type(),
                                    presentAndUntaggedVersions,
                                    field.nullableVersions());
                            } else {
                                buffer.printf("%s;%n",
                                    primitiveWriteExpression(field.type(), field.camelCaseName()));
                            }
                        }).
                        ifMember((__) -> {
                            generateNonDefaultValueCheck(field);
                            buffer.incrementIndent();
                            buffer.printf("_numTaggedFields++;%n");
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                            if (taggedFields.put(field.tag().get(), field) != null) {
                                throw new RuntimeException("Field " + field.name() + " has tag " +
                                    field.tag() + ", but another field already used that tag.");
                            }
                        }).
                        generate(buffer);
                });
                if (!field.ignorable()) {
                    cond.ifNotMember((__) -> {
                        generateNonDefaultValueCheck(field);
                        buffer.incrementIndent();
                        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                        buffer.printf("throw new UnsupportedVersionException(" +
                                "\"Attempted to write a non-default %s at version \" + version);%n",
                            field.camelCaseName());
                        buffer.decrementIndent();
                        buffer.printf("}%n");
                    });
                }
                cond.generate(buffer);
        }
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_WRITER_CLASS);
        buffer.printf("RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);%n");
        buffer.printf("_numTaggedFields += _rawWriter.numFields();%n");
        VersionConditional.forVersions(flexibleVersions, curVersions).
            ifNotMember((__) -> {
                buffer.printf("if (_numTaggedFields > 0) {%n");
                buffer.incrementIndent();
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Tagged fields were set, " +
                    "but version \" + version + \" of this message does not support them.\");%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            ifMember((__) -> {
                int prevTag = -1;
                for (FieldSpec field : taggedFields.values()) {
                    if (prevTag + 1 != field.tag().get()) {
                        buffer.printf("_rawWriter.writeRawTags(writable, %d);%n", field.tag().get());
                    }
                    VersionConditional.
                        forVersions(field.versions(), field.taggedVersions().intersect(field.versions())).
                        allowMembershipCheckAlwaysFalse(false).
                        ifMember((presentAndTaggedVersions) -> {
                            generateNonDefaultValueCheck(field);
                            buffer.incrementIndent();
                            buffer.printf("writable.writeUnsignedVarint(%d);%n", field.tag().get());
                            if (field.type().isString()) {
                                IsNullConditional.forField(field.camelCaseName()).
                                    nullableVersions(field.nullableVersions()).
                                    possibleVersions(presentAndTaggedVersions).
                                    ifNull((___) -> {
                                        buffer.printf("writable.writeUnsignedVarint(0);%n");
                                    }).
                                    ifNotNull((___) -> {
                                        buffer.printf("byte[] _stringBytes = sizeCache.getSerializedValue(this.%s);%n",
                                            field.camelCaseName());
                                        buffer.printf("writable.writeUnsignedVarint(_stringBytes.length);%n");
                                        buffer.printf("writable.writeByteArray(_stringBytes);%n");
                                    }).
                                    generate(buffer);
                            } else if (field.type().isVariableLength()) {
                                if (field.type().isArray()) {
                                    buffer.printf("writable.writeUnsignedVarint(sizeCache.get(this.%s));%n",
                                        field.camelCaseName());
                                } else if (field.type().isBytes()) {
                                    buffer.printf("writable.writeUnsignedVarint(this.%s.length);%n",
                                        field.camelCaseName());
                                } else {
                                    throw new RuntimeException("Unable to handle type " + field.type());
                                }
                                generateVariableLengthWriter(field.camelCaseName(),
                                    field.type(),
                                    presentAndTaggedVersions,
                                    field.nullableVersions());
                            } else {
                                buffer.printf("writable.writeUnsignedVarint(%d);%n",
                                    field.type().fixedLength().get());
                                buffer.printf("%s;%n",
                                    primitiveWriteExpression(field.type(), field.camelCaseName()));
                            }
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                        }).
                        generate(buffer);
                    prevTag = field.tag().get();
                }
                if (prevTag < Integer.MAX_VALUE) {
                    buffer.printf("_rawWriter.writeRawTags(writable, Integer.MAX_VALUE);%n");
                }
            }).
            generate(buffer);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String primitiveWriteExpression(FieldType type, String name) {
        if (type instanceof FieldType.BoolFieldType) {
            return String.format("writable.writeByte(%s ? (byte) 1 : (byte) 0)", name);
        } else if (type instanceof FieldType.Int8FieldType) {
            return String.format("writable.writeByte(%s)", name);
        } else if (type instanceof FieldType.Int16FieldType) {
            return String.format("writable.writeShort(%s)", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("writable.writeInt(%s)", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("writable.writeLong(%s)", name);
        } else if (type instanceof FieldType.StructType) {
            return String.format("%s.write(writable, sizeCache, version)", name);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateVariableLengthWriter(String name,
                                              FieldType type,
                                              Versions versions,
                                              Versions nullableVersions) {
        IsNullConditional.forField(name).
            possibleVersions(versions).
            nullableVersions(nullableVersions).
            alwaysEmitBlockScope(type.isString()).
            ifNull((ifNullVersions) -> {
                VersionConditional.forVersions(flexibleVersions, ifNullVersions).
                    ifMember((__) -> {
                        buffer.printf("writable.writeUnsignedVarint(0);%n");
                    }).
                    ifNotMember((__) -> {
                        if (type.isString()) {
                            buffer.printf("writable.writeShort((short) -1);%n");
                        } else {
                            buffer.printf("writable.writeInt(-1);%n");
                        }
                    }).
                    generate(buffer);
            }).
            ifNotNull((ifNotNullVersions) -> {
                final String lengthExpression;
                if (type.isString()) {
                    lengthExpression = "arr.length";
                    headerGenerator.addImport(MessageGenerator.STANDARD_CHARSETS);
                    buffer.printf("byte[] arr = %s.getBytes(StandardCharsets.UTF_8);%n", name);
                    buffer.printf("if (%s > 0x7fff) {%n", lengthExpression);
                    buffer.incrementIndent();
                    buffer.printf("throw new RuntimeException(\"'%s' field is too long to " +
                        "be serialized\");%n", name);
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                } else if (type.isBytes()) {
                    lengthExpression = String.format("%s.length", name);
                } else if (type.isArray()) {
                    lengthExpression = String.format("%s.size()", name);
                } else {
                    throw new RuntimeException("Unhandled type " + type);
                }
                VersionConditional.forVersions(flexibleVersions, ifNotNullVersions).
                    ifMember((__) -> {
                        buffer.printf("writable.writeUnsignedVarint(%s + 1);%n", lengthExpression);
                    }).
                    ifNotMember((__) -> {
                        if (type.isString()) {
                            buffer.printf("writable.writeShort((short) %s);%n", lengthExpression);
                        } else {
                            buffer.printf("writable.writeInt(%s);%n", lengthExpression);
                        }
                    }).
                    generate(buffer);
                if (type.isString()) {
                    buffer.printf("writable.writeByteArray(arr);%n");
                } else if (type.isBytes()) {
                    buffer.printf("writable.writeByteArray(%s);%n", name);
                } else if (type.isArray()) {
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                    FieldType elementType = arrayType.elementType();
                    String elementName = String.format("%sElement", name);
                    buffer.printf("for (%s %s : %s) {%n",
                        getBoxedJavaType(elementType),
                        elementName,
                        name);
                    buffer.incrementIndent();
                    if (elementType.isArray()) {
                        throw new RuntimeException("Nested arrays are not supported.  " +
                            "Use an array of structures containing another array.");
                    } else if (elementType.isBytes() || elementType.isString()) {
                        generateVariableLengthWriter(elementName,
                            elementType,
                            versions,
                            Versions.NONE);
                    } else {
                        buffer.printf("%s;%n", primitiveWriteExpression(elementType, elementName));
                    }
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                } else {
                    throw new RuntimeException("Can't handle variable length type " + type);
                }
            }).
            generate(buffer);
    }

    private void generateNonDefaultValueCheck(FieldSpec field) {
        if (field.type().isArray()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else {
                buffer.printf("if (!%s.isEmpty()) {%n", field.camelCaseName());
            }
        } else if (field.type().isBytes()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else {
                buffer.printf("if (%s.length != 0) {%n", field.camelCaseName());
            }
        } else if (field.type().isString()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else {
                buffer.printf("if (!%s.equals(%s)) {%n", field.camelCaseName(), fieldDefault(field));
            }
        } else if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if (%s%s) {%n",
                fieldDefault(field).equals("true") ? "!" : "",
                field.camelCaseName());
        } else {
            buffer.printf("if (%s != %s) {%n", field.camelCaseName(), fieldDefault(field));
        }
    }

    private void generateClassToStruct(String className, StructSpec struct,
                                       Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public Struct toStruct(short version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember((__) -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                    "version \" + version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        buffer.printf("Struct struct = new Struct(SCHEMAS[version]);%n");
        for (FieldSpec field : struct.fields()) {
            VersionConditional.forVersions(curVersions, field.versions()).
                alwaysEmitBlockScope(field.type().isArray()).
                ifMember((presentVersions) -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember((presentAndUntaggedVersions) -> {
                            generateFieldToStruct(field, presentAndUntaggedVersions);
                        }).
                        generate(buffer);
                }).
                generate(buffer);
        }
        buffer.printf("return struct;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldToStruct(FieldSpec field, Versions versions) {
        if ((!field.type().canBeNullable()) &&
            (!field.nullableVersions().empty())) {
            throw new RuntimeException("Fields of type " + field.type() +
                " cannot be nullable.");
        }
        if ((field.type() instanceof FieldType.BoolFieldType) ||
                (field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Int32FieldType) ||
                (field.type() instanceof FieldType.Int64FieldType) ||
                (field.type() instanceof FieldType.StringFieldType)) {
            buffer.printf("struct.set(\"%s\", this.%s);%n",
                field.snakeCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            buffer.printf("struct.setByteArray(\"%s\", this.%s);%n",
                field.snakeCaseName(), field.camelCaseName());
        } else if (field.type().isArray()) {
            IsNullConditional.forField(field).
                possibleVersions(versions).
                ifNull((__) -> {
                    buffer.printf("struct.set(\"%s\", null);%n", field.snakeCaseName());
                }).
                ifNotNull((__) -> {
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    FieldType elementType = arrayType.elementType();
                    String boxdElementType = elementType.isStruct() ? "Struct" : getBoxedJavaType(elementType);
                    buffer.printf("%s[] nestedObjects = new %s[%s.size()];%n",
                        boxdElementType, boxdElementType, field.camelCaseName());
                    buffer.printf("int i = 0;%n");
                    buffer.printf("for (%s element : this.%s) {%n",
                        getBoxedJavaType(arrayType.elementType()), field.camelCaseName());
                    buffer.incrementIndent();
                    if (elementType.isStruct()) {
                        buffer.printf("nestedObjects[i++] = element.toStruct(version);%n");
                    } else {
                        buffer.printf("nestedObjects[i++] = element;%n");
                    }
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                    buffer.printf("struct.set(\"%s\", (Object[]) nestedObjects);%n",
                        field.snakeCaseName());
                }).generate(buffer);
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateClassSize(String className, StructSpec struct,
                                   Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.OBJECT_SIZE_CACHE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public int size(ObjectSizeCache sizeCache, short version) {%n");
        buffer.incrementIndent();
        buffer.printf("int size = 0;%n");
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember((__) -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't size " +
                    "version \" + version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            VersionConditional.forVersions(field.versions(), curVersions).
                ifMember((presentVersions) -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember((presentAndUntaggedVersions) -> {
                            generateFieldSize(field, presentAndUntaggedVersions);
                        }).
                        generate(buffer);
                }).generate(buffer);
        }
        buffer.printf("return size;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateVariableLengthFieldSize(String fieldName, FieldType type, boolean nullable) {
        if (type instanceof FieldType.StringFieldType) {
            buffer.printf("size += 2;%n");
            if (nullable) {
                buffer.printf("if (%s != null) {%n", fieldName);
                buffer.incrementIndent();
            }
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("size += MessageUtil.serializedUtf8Length(%s);%n", fieldName);
            if (nullable) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (type instanceof FieldType.BytesFieldType) {
            buffer.printf("size += 4;%n");
            if (nullable) {
                buffer.printf("if (%s != null) {%n", fieldName);
                buffer.incrementIndent();
            }
            buffer.printf("size += %s.length;%n", fieldName);
            if (nullable) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (type instanceof FieldType.StructType) {
            buffer.printf("size += %s.size(sizeCache, version);%n", fieldName);
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
    }

    private void generateFieldSize(FieldSpec field, Versions possibleVersions) {
        if (field.type().fixedLength().isPresent()) {
            buffer.printf("size += %d;%n", field.type().fixedLength().get());
            return;
        }
        IsNullConditional.forField(field).
            alwaysEmitBlockScope(field.type().isString()).
            possibleVersions(possibleVersions).
            ifNull((ifNullVersions) -> {
                VersionConditional.forVersions(flexibleVersions, ifNullVersions).
                    ifMember((__) -> {
                        buffer.printf("size += 1;%n");
                    }).
                    ifNotMember((__) -> {
                        if (field.type().isString()) {
                            buffer.printf("size += 2;%n");
                        } else {
                            buffer.printf("size += 4;%n");
                        }
                    }).
                    generate(buffer);
            }).
            ifNotNull((ifNotNullVersions) -> {
                if (field.type().isString()) {
                    headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
                    buffer.printf("int stringLength = MessageUtil.serializedUtf8Length(%s);%n",
                        field.camelCaseName());
                    buffer.printf("size += stringLength;%n");
                    VersionConditional.forVersions(flexibleVersions, ifNotNullVersions).
                        ifMember((__) -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("size += ByteUtils.sizeOfUnsignedVarint(stringLength + 1);%n");
                        }).
                        ifNotMember((__) -> {
                            buffer.printf("size += 2;%n");
                        }).
                        generate(buffer);
                } else if (field.type().isArray()) {
                    VersionConditional.forVersions(flexibleVersions, ifNotNullVersions).
                        ifMember((__) -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("size += ByteUtils.sizeOfUnsignedVarint(%s.size() + 1);%n",
                                field.camelCaseName());
                        }).
                        ifNotMember((__) -> {
                            buffer.printf("size += 4;%n");
                        }).
                        generate(buffer);
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    FieldType elementType = arrayType.elementType();
                    if (elementType.fixedLength().isPresent()) {
                        buffer.printf("size += %s.size() * %d;%n",
                            field.camelCaseName(),
                            elementType.fixedLength().get());
                    } else if (elementType instanceof FieldType.ArrayType) {
                        throw new RuntimeException("Arrays of arrays are not supported " +
                            "(use a struct).");
                    } else {
                        buffer.printf("for (%s element : %s) {%n",
                            getBoxedJavaType(elementType), field.camelCaseName());
                        buffer.incrementIndent();
                        generateVariableLengthFieldSize("element", elementType, false);
                        buffer.decrementIndent();
                        buffer.printf("}%n");
                    }
                } else if (field.type().isBytes()) {
                    buffer.printf("size += %s.length;%n", field.camelCaseName());
                    VersionConditional.forVersions(flexibleVersions, ifNotNullVersions).
                        ifMember((__) -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("size += ByteUtils.sizeOfUnsignedVarint(%s.length + 1);%n",
                                field.camelCaseName());
                        }).
                        ifNotMember((__) -> {
                            buffer.printf("size += 4;%n");
                        }).
                        generate(buffer);
                } else {
                    throw new RuntimeException("unhandled type " + field.type());
                }
            }).
            generate(buffer);
    }

    private void generateClassEquals(String className, StructSpec struct, boolean onlyMapKeys) {
        buffer.printf("@Override%n");
        buffer.printf("public boolean equals(Object obj) {%n");
        buffer.incrementIndent();
        buffer.printf("if (!(obj instanceof %s)) return false;%n", className);
        if (!struct.fields().isEmpty()) {
            buffer.printf("%s other = (%s) obj;%n", className, className);
            for (FieldSpec field : struct.fields()) {
                if ((!onlyMapKeys) || field.mapKey()) {
                    generateFieldEquals(field);
                }
            }
        }
        buffer.printf("return true;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldEquals(FieldSpec field) {
        if (field.type().isString() || field.type().isArray() || field.type().isStruct()) {
            buffer.printf("if (this.%s == null) {%n", field.camelCaseName());
            buffer.incrementIndent();
            buffer.printf("if (other.%s != null) return false;%n", field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            buffer.printf("if (!this.%s.equals(other.%s)) return false;%n",
                field.camelCaseName(), field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (field.type().isBytes()) {
            // Arrays#equals handles nulls.
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("if (!Arrays.equals(this.%s, other.%s)) return false;%n",
                field.camelCaseName(), field.camelCaseName());
        } else {
            buffer.printf("if (%s != other.%s) return false;%n",
                field.camelCaseName(), field.camelCaseName());
        }
    }

    private void generateClassHashCode(StructSpec struct, boolean onlyMapKeys) {
        buffer.printf("@Override%n");
        buffer.printf("public int hashCode() {%n");
        buffer.incrementIndent();
        buffer.printf("int hashCode = 0;%n");
        for (FieldSpec field : struct.fields()) {
            if ((!onlyMapKeys) || field.mapKey()) {
                generateFieldHashCode(field);
            }
        }
        buffer.printf("return hashCode;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldHashCode(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("hashCode = 31 * hashCode + (%s ? 1231 : 1237);%n",
                field.camelCaseName());
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                    (field.type() instanceof FieldType.Int16FieldType) ||
                    (field.type() instanceof FieldType.Int32FieldType)) {
            buffer.printf("hashCode = 31 * hashCode + %s;%n",
                field.camelCaseName());
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            buffer.printf("hashCode = 31 * hashCode + ((int) (%s >> 32) ^ (int) %s);%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isString()) {
            buffer.printf("hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("hashCode = 31 * hashCode + Arrays.hashCode(%s);%n",
                field.camelCaseName());
        } else if (field.type().isStruct() || field.type().isArray()) {
            buffer.printf("hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                field.camelCaseName(), field.camelCaseName());
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateClassToString(String className, StructSpec struct) {
        buffer.printf("@Override%n");
        buffer.printf("public String toString() {%n");
        buffer.incrementIndent();
        buffer.printf("return \"%s(\"%n", className);
        buffer.incrementIndent();
        String prefix = "";
        for (FieldSpec field : struct.fields()) {
            generateFieldToString(prefix, field);
            prefix = ", ";
        }
        buffer.printf("+ \")\";%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldToString(String prefix, FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("+ \"%s%s=\" + (%s ? \"true\" : \"false\")%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Int32FieldType) ||
                (field.type() instanceof FieldType.Int64FieldType)) {
            buffer.printf("+ \"%s%s=\" + %s%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isString()) {
            buffer.printf("+ \"%s%s='\" + %s + \"'\"%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("+ \"%s%s=\" + Arrays.toString(%s)%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isStruct()) {
            buffer.printf("+ \"%s%s=\" + %s.toString()%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isArray()) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            if (field.nullableVersions().empty()) {
                buffer.printf("+ \"%s%s=\" + MessageUtil.deepToString(%s.iterator())%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
            } else {
                buffer.printf("+ \"%s%s=\" + ((%s == null) ? \"null\" : " +
                    "MessageUtil.deepToString(%s.iterator()))%n",
                    prefix, field.camelCaseName(), field.camelCaseName(), field.camelCaseName());
            }
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private String fieldDefault(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            if (field.defaultString().isEmpty()) {
                return "false";
            } else if (field.defaultString().equalsIgnoreCase("true")) {
                return "true";
            } else if (field.defaultString().equalsIgnoreCase("false")) {
                return "false";
            } else {
                throw new RuntimeException("Invalid default for boolean field " +
                    field.name() + ": " + field.defaultString());
            }
        } else if (field.type() instanceof FieldType.Int8FieldType) {
            if (field.defaultString().isEmpty()) {
                return "(byte) 0";
            } else {
                try {
                    Byte.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int8 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return "(byte) " + field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int16FieldType) {
            if (field.defaultString().isEmpty()) {
                return "(short) 0";
            } else {
                try {
                    Short.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int16 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return "(short) " + field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int32FieldType) {
            if (field.defaultString().isEmpty()) {
                return "0";
            } else {
                try {
                    Integer.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int32 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            if (field.defaultString().isEmpty()) {
                return "0L";
            } else {
                try {
                    Integer.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int64 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return field.defaultString() + "L";
            }
        } else if (field.type() instanceof FieldType.StringFieldType) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else {
                return "\"" + field.defaultString() + "\"";
            }
        } else if (field.type().isBytes()) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for bytes field " +
                        field.name() + ".  The only valid default for a bytes field " +
                        "is empty or null.");
            }
            headerGenerator.addImport(MessageGenerator.BYTES_CLASS);
            return "Bytes.EMPTY";
        } else if (field.type().isStruct()) {
            if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for struct field " +
                    field.name() + ": custom defaults are not supported for struct fields.");
            }
            return "new " + field.type().toString() + "()";
        } else if (field.type().isArray()) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for array field " +
                    field.name() + ".  The only valid default for an array field " +
                        "is the empty array or null.");
            }
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (structRegistry.isStructArrayWithKeys(field)) {
                return "new " + collectionType(arrayType.elementType().toString()) + "(0)";
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                return "new ArrayList<" + getBoxedJavaType(arrayType.elementType()) + ">()";
            }
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void validateNullDefault(FieldSpec field) {
        if (!(field.nullableVersions().contains(field.versions()))) {
            throw new RuntimeException("null cannot be the default for field " +
                    field.name() + ", because not all versions of this field are " +
                    "nullable.");
        }
    }

    private void generateFieldAccessor(FieldSpec field) {
        buffer.printf("%n");
        generateAccessor(fieldAbstractJavaType(field), field.camelCaseName(),
            field.camelCaseName());
    }

    private void generateAccessor(String javaType, String functionName, String memberName) {
        buffer.printf("public %s %s() {%n", javaType, functionName);
        buffer.incrementIndent();
        buffer.printf("return this.%s;%n", memberName);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldMutator(String className, FieldSpec field) {
        buffer.printf("%n");
        buffer.printf("public %s set%s(%s v) {%n",
            className,
            field.capitalizedCamelCaseName(),
            fieldAbstractJavaType(field));
        buffer.incrementIndent();
        buffer.printf("this.%s = v;%n", field.camelCaseName());
        buffer.printf("return this;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateSetter(String javaType, String functionName, String memberName) {
        buffer.printf("public void %s(%s v) {%n", functionName, javaType);
        buffer.incrementIndent();
        buffer.printf("this.%s = v;%n", memberName);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }
}
