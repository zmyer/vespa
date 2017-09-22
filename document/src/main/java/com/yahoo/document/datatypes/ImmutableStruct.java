// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.document.datatypes;

import com.yahoo.document.Field;
import com.yahoo.document.StructDataType;
import com.yahoo.document.serialization.FieldReader;
import com.yahoo.document.serialization.FieldWriter;
import com.yahoo.document.serialization.VespaDocumentDeserializerHead;
import com.yahoo.document.serialization.VespaDocumentSerializerHead;
import com.yahoo.document.serialization.XmlStream;
import com.yahoo.io.GrowableByteBuffer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author baldersheim
 */
public final class ImmutableStruct extends StructuredFieldValue {
    private int fieldCount = 0;
    private final Field [] fields;
    private final int [] offsets;
    private final GrowableByteBuffer buffer;
    private final VespaDocumentSerializerHead serializer;

    public ImmutableStruct(StructDataType dataType, int maxFieldCount, GrowableByteBuffer backing) {
        super(dataType);
        buffer = (backing == null) ? new GrowableByteBuffer(1024) : backing;
        serializer = new VespaDocumentSerializerHead(buffer);
        fields = new Field[maxFieldCount];
        offsets = new int[maxFieldCount+1];
        offsets[0] = buffer.position();
    }
    private int findField(Field id) {
        int index = -1;
        for (int i = 0; i < fieldCount; i++) {
            if (fields[i].getId() == id.getId()) {
                return i;
            }
        }
        return index;
    }
    @Override
    public StructDataType getDataType() {
        return (StructDataType)super.getDataType();
    }
    @Override
    public Field getField(String fieldName) {
        return getDataType().getField(fieldName);
    }

    private int length(int index) {
        return offsets[index+1] - offsets[index];
    }
    private FieldReader getReader(int index) {
        return new VespaDocumentDeserializerHead(null,
                GrowableByteBuffer.wrap(buffer.array(), offsets[index], length(index)));
    }
    @Override
    public FieldValue getFieldValue(Field field) {
        int index = findField(field);
        if (index < 0) { return null; }
        FieldValue fv = field.getDataType().createFieldValue();
        fv.deserialize(field, getReader(index));
        return fv;
    }

    public FieldValue setFieldValue(Field field, FieldValue value) {
        doSetFieldValue(field, value);
        return null;
    }


        @Override
    protected void doSetFieldValue(Field field, FieldValue value) {
        int index = findField(field);
        if (index >= 0) {
            throw new IllegalStateException("Field " + field + " already exist at index " + index);
        }

        value.serialize(null, serializer);
        fields[fieldCount++] = field;
        offsets[fieldCount] = buffer.position();
    }

    @Override
    public FieldValue removeFieldValue(Field field) {
        throw new IllegalStateException("No removeFieldValue yet");
    }

    @Override
    public void printXml(XmlStream xml) {
        throw new IllegalStateException("No xml yet");
    }

    @Override
    public void clear() {
        fieldCount = 0;
        buffer.clear();
    }

    @Override
    public void assign(Object o) {
        throw new IllegalStateException("ImmutableStruct is immutable....");
    }

    @Override
    public void serialize(Field field, FieldWriter writer) {
        writer.write(field, this);
    }

    @Override
    public void deserialize(Field field, FieldReader reader) {
        throw new IllegalStateException("No deserialize yet");
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    private static class FieldEntry implements Map.Entry<Field, FieldValue> {
        private final Field field;
        private final FieldValue value;

        private FieldEntry(Field f, FieldValue fv) {
            field = f;
            value = fv;
        }

        public Field getKey() {
            return field;
        }

        public FieldValue getValue() {
            return value;
        }

        public FieldValue setValue(FieldValue value) {
            throw new IllegalStateException("setValue not implemented yet");
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldEntry)) return false;

            FieldEntry that = (FieldEntry) o;
            return (field.equals(that.field) && value.equals(that.value));
        }

        public int hashCode() {
            return field.hashCode() + value.hashCode();
        }
    }

    private final class MyIterator implements Iterator<Map.Entry<Field, FieldValue>> {
        private final VespaDocumentDeserializerHead reader;
        private int index;

        MyIterator() {
            reader = new VespaDocumentDeserializerHead(null, GrowableByteBuffer.wrap(buffer.array()));
            index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < getFieldCount();
        }

        @Override
        public Map.Entry<Field, FieldValue> next() {
            int i = index++;
            Field field = fields[i];
            FieldValue fv = field.getDataType().createFieldValue();
            fv.deserialize(field, reader);
            return new FieldEntry(field, fv);
        }
    }
    @Override
    public Iterator<Map.Entry<Field, FieldValue>> iterator() {
        return new MyIterator();
    }
    private final class Compare implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            int a = fields[o1].getId();
            int b = fields[o2].getId();
            return a - b;
        }
    }
    public GrowableByteBuffer getRawBuffer(List<Integer> fieldIds, List<Integer> fieldLengths) {
        GrowableByteBuffer buf = new GrowableByteBuffer(offsets[fieldCount] - offsets[0], 2.0f);
        Integer [] order = new Integer[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            order[i] = i;
        }
        Arrays.sort(order, new Compare());

        byte [] data = buffer.array();
        for (int index : order) {
            int startPos = buf.position();
            buf.put(data, offsets[index], length(index));

            fieldLengths.add(buf.position() - startPos);
            fieldIds.add(fields[index].getId());
        }

        buf.flip();
        return buf;
    }
}
