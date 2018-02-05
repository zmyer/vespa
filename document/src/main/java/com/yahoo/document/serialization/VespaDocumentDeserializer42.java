// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.document.serialization;

import com.yahoo.collections.Tuple2;
import com.yahoo.compress.CompressionType;
import com.yahoo.compress.Compressor;
import com.yahoo.document.ArrayDataType;
import com.yahoo.document.CollectionDataType;
import com.yahoo.document.DataType;
import com.yahoo.document.DataTypeName;
import com.yahoo.document.Document;
import com.yahoo.document.DocumentId;
import com.yahoo.document.DocumentType;
import com.yahoo.document.DocumentTypeManager;
import com.yahoo.document.DocumentUpdate;
import com.yahoo.document.Field;
import com.yahoo.document.MapDataType;
import com.yahoo.document.StructDataType;
import com.yahoo.document.WeightedSetDataType;
import com.yahoo.document.annotation.AlternateSpanList;
import com.yahoo.document.annotation.Annotation;
import com.yahoo.document.annotation.AnnotationReference;
import com.yahoo.document.annotation.AnnotationType;
import com.yahoo.document.annotation.Span;
import com.yahoo.document.annotation.SpanList;
import com.yahoo.document.annotation.SpanNode;
import com.yahoo.document.annotation.SpanNodeParent;
import com.yahoo.document.annotation.SpanTree;
import com.yahoo.document.datatypes.Array;
import com.yahoo.document.datatypes.ByteFieldValue;
import com.yahoo.document.datatypes.CollectionFieldValue;
import com.yahoo.document.datatypes.DoubleFieldValue;
import com.yahoo.document.datatypes.FieldValue;
import com.yahoo.document.datatypes.FloatFieldValue;
import com.yahoo.document.datatypes.IntegerFieldValue;
import com.yahoo.document.datatypes.LongFieldValue;
import com.yahoo.document.datatypes.MapFieldValue;
import com.yahoo.document.datatypes.PredicateFieldValue;
import com.yahoo.document.datatypes.Raw;
import com.yahoo.document.datatypes.ReferenceFieldValue;
import com.yahoo.document.datatypes.StringFieldValue;
import com.yahoo.document.datatypes.Struct;
import com.yahoo.document.datatypes.StructuredFieldValue;
import com.yahoo.document.datatypes.TensorFieldValue;
import com.yahoo.document.datatypes.WeightedSet;
import com.yahoo.document.fieldpathupdate.AddFieldPathUpdate;
import com.yahoo.document.fieldpathupdate.AssignFieldPathUpdate;
import com.yahoo.document.fieldpathupdate.FieldPathUpdate;
import com.yahoo.document.fieldpathupdate.RemoveFieldPathUpdate;
import com.yahoo.document.predicate.BinaryFormat;
import com.yahoo.document.select.parser.ParseException;
import com.yahoo.document.update.AddValueUpdate;
import com.yahoo.document.update.ArithmeticValueUpdate;
import com.yahoo.document.update.AssignValueUpdate;
import com.yahoo.document.update.ClearValueUpdate;
import com.yahoo.document.update.FieldUpdate;
import com.yahoo.document.update.MapValueUpdate;
import com.yahoo.document.update.RemoveValueUpdate;
import com.yahoo.document.update.ValueUpdate;
import com.yahoo.io.GrowableByteBuffer;
import com.yahoo.tensor.serialization.TypedBinaryFormat;
import com.yahoo.text.Utf8;
import com.yahoo.text.Utf8Array;
import com.yahoo.text.Utf8String;
import com.yahoo.vespa.objects.FieldBase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.text.Utf8.calculateStringPositions;

/**
 * Class used for de-serializing documents on the Vespa 4.2 document format.
 *
 * @deprecated Please use {@link com.yahoo.document.serialization.VespaDocumentDeserializerHead} instead for new code.
 * @author baldersheim
 */
@Deprecated // Remove on Vespa 7
// When removing: Move content of this class into VespaDocumentDeserializerHead
public class VespaDocumentDeserializer42 extends VespaDocumentSerializer42 implements DocumentDeserializer {

    private final Compressor compressor = new Compressor();
    private DocumentTypeManager manager;
    GrowableByteBuffer body;
    private short version;
    private List<SpanNode> spanNodes;
    private List<Annotation> annotations;
    private int[] stringPositions;

    VespaDocumentDeserializer42(DocumentTypeManager manager, GrowableByteBuffer header, GrowableByteBuffer body, short version) {
        super(header);
        this.manager = manager;
        this.body = body;
        this.version = version;
    }

    VespaDocumentDeserializer42(DocumentTypeManager manager, GrowableByteBuffer buf) {
        this(manager, buf, null, Document.SERIALIZED_VERSION);
    }

    VespaDocumentDeserializer42(DocumentTypeManager manager, GrowableByteBuffer buf, GrowableByteBuffer body) {
        this(manager, buf, body, Document.SERIALIZED_VERSION);
    }

    final public DocumentTypeManager getDocumentTypeManager() { return manager; }

    public void read(Document document) {
         read(null, document);
    }
    public void read(FieldBase field, Document doc) {

        // Verify that we have correct version
        version = getShort(null);
        if (version < 6 || version > Document.SERIALIZED_VERSION) {
            throw new DeserializationException("Unknown version " + version + ", expected " + 
                                               Document.SERIALIZED_VERSION + ".");
        }

        int dataLength = 0;
        int dataPos = 0;

        if (version < 7) {
            getInt2_4_8Bytes(null); // Total document size.. Ignore
        } else {
            dataLength = getInt(null);
            dataPos = position();
        }

        doc.setId(readDocumentId());

        Byte content = getByte(null);

        doc.setDataType(readDocumentType());

        if ((content & 0x2) != 0) {
            doc.getHeader().deserialize(new Field("header"),this);
        }
        if ((content & 0x4) != 0) {
            doc.getBody().deserialize(new Field("body"),this);
        } else if (body != null) {
            GrowableByteBuffer header = getBuf();
            setBuf(body);
            body = null;
            doc.getBody().deserialize(new Field("body"), this);
            body = getBuf();
            setBuf(header);
        }

        if (version < 8) {
            int crcVal = getInt(null);
        }

        if (version > 6) {
            if (dataLength != (position() - dataPos)) {
                throw new DeserializationException("Length mismatch");
            }
        }
    }
    public void read(FieldBase field, FieldValue value) {
        throw new IllegalArgumentException("read not implemented yet.");
    }

    public <T extends FieldValue> void read(FieldBase field, Array<T> array) {
        int numElements = getNumCollectionElems();
        ArrayList<T> list = new ArrayList<T>(numElements);
        ArrayDataType type = array.getDataType();
        for (int i = 0; i < numElements; i++) {
            if (version < 7) {
                getInt(null); // We don't need size for anything
            }
            FieldValue fv = type.getNestedType().createFieldValue();
            fv.deserialize(null, this);
            list.add((T) fv);
        }
        array.clear();
        array.addAll(list);
    }

    public <K extends FieldValue, V extends FieldValue> void read(FieldBase field, MapFieldValue<K, V> map) {
        int numElements = getNumCollectionElems();
        Map<K,V> hash = new HashMap<>();
        MapDataType type = map.getDataType();
        for (int i = 0; i < numElements; i++) {
            if (version < 7) {
                getInt(null); // We don't need size for anything
            }
            K key = (K) type.getKeyType().createFieldValue();
            V val = (V) type.getValueType().createFieldValue();
            key.deserialize(null, this);
            val.deserialize(null, this);
            hash.put(key, val);
        }
        map.clear();
        map.putAll(hash);
    }

    private int getNumCollectionElems() {
        int numElements;
        if (version < 7) {
            getInt(null); // We already know the nested type, so ignore that..
            numElements = getInt(null);
        } else {
            numElements = getInt1_2_4Bytes(null);
        }
        if (numElements < 0) {
            throw new DeserializationException("Bad number of array/map elements, " + numElements);
        }
        return numElements;
    }

    public <T extends FieldValue> void read(FieldBase field, CollectionFieldValue<T> value) {
        throw new IllegalArgumentException("read not implemented yet.");
    }
    public void read(FieldBase field, ByteFieldValue value)    { value.assign(getByte(null)); }
    public void read(FieldBase field, DoubleFieldValue value)  { value.assign(getDouble(null)); }
    public void read(FieldBase field, FloatFieldValue value)   { value.assign(getFloat(null)); }
    public void read(FieldBase field, IntegerFieldValue value) { value.assign(getInt(null)); }
    public void read(FieldBase field, LongFieldValue value)    { value.assign(getLong(null)); }

    public void read(FieldBase field, Raw value) {
        int rawsize = getInt(null);
        byte[] rawBytes = getBytes(null, rawsize);
        value.assign(rawBytes);
    }

    @Override
    public void read(FieldBase field, PredicateFieldValue value) {
        int len = getInt(null);
        byte[] buf = getBytes(null, len);
        value.assign(BinaryFormat.decode(buf));
    }

    public void read(FieldBase field, StringFieldValue value) {
        byte coding = getByte(null);

        int length = getInt1_4Bytes(null);

        //OK, it seems that this length includes null termination.
        //NOTE: the following four lines are basically parseNullTerminatedString() inlined,
        //but we need to use the UTF-8 buffer below, so not using that method...
        byte[] stringArray = new byte[length - 1];
        buf.get(stringArray);
        buf.get();    //move past 0-termination
        value.setUnChecked(Utf8.toString(stringArray));

        if ((coding & 64) == 64) {
            //we have a span tree!
            try {
                //we don't support serialization of nested span trees, so this is safe:
                stringPositions = calculateStringPositions(stringArray);
                //total length:
                int size = buf.getInt();
                int startPos = buf.position();

                int numSpanTrees = buf.getInt1_2_4Bytes();

                for (int i = 0; i < numSpanTrees; i++) {
                    SpanTree tree = new SpanTree();
                    StringFieldValue treeName = new StringFieldValue();
                    treeName.deserialize(this);
                    tree.setName(treeName.getString());
                    value.setSpanTree(tree);
                    readSpanTree(tree, false);
                }

                buf.position(startPos + size);
            } finally {
                stringPositions = null;
            }
        }
    }

    @Override
    public void read(FieldBase field, TensorFieldValue value) {
        int encodedTensorLength = buf.getInt1_4Bytes();
        if (encodedTensorLength > 0) {
            byte[] encodedTensor = getBytes(null, encodedTensorLength);
            value.assign(TypedBinaryFormat.decode(Optional.of(value.getDataType().getTensorType()), 
                                                  GrowableByteBuffer.wrap(encodedTensor)));
        } else {
            value.clear();
        }
    }

    @Override
    public void read(FieldBase field, ReferenceFieldValue value) {
        final boolean documentIdPresent = (buf.get() != 0);
        if (documentIdPresent) {
            value.assign(readDocumentId());
        } else {
            value.clear();
        }
    }

    public void read(FieldBase fieldDef, Struct s) {
        s.setVersion(version);
        int startPos = position();

        if (version < 6) {
            throw new DeserializationException("Illegal document serialization version " + version);
        }

        int dataSize;
        if (version < 7) {
            long rSize = getInt2_4_8Bytes(null);
            //TODO: Look into how to support data segments larger than INT_MAX bytes
            if (rSize > Integer.MAX_VALUE) {
                throw new DeserializationException("Raw size of data block is too large.");
            }
            dataSize = (int)rSize;
        } else {
            dataSize = getInt(null);
        }

        byte comprCode = getByte(null);
        CompressionType compression = CompressionType.valueOf(comprCode);

        int uncompressedSize = 0;
        if (compression != CompressionType.NONE &&
            compression != CompressionType.INCOMPRESSIBLE)
        {
            // uncompressedsize (full size of FIELDS only, after decompression)
            long pSize = getInt2_4_8Bytes(null);
            //TODO: Look into how to support data segments larger than INT_MAX bytes
            if (pSize > Integer.MAX_VALUE) {
                throw new DeserializationException("Uncompressed size of data block is too large.");
            }
            uncompressedSize = (int) pSize;
        }

        int numberOfFields = getInt1_4Bytes(null);

        List<Tuple2<Integer, Long>> fieldIdsAndLengths = new ArrayList<>(numberOfFields);
        for (int i=0; i<numberOfFields; ++i) {
            // id, length (length only used for unknown fields
            fieldIdsAndLengths.add(new Tuple2<>(getInt1_4Bytes(null), getInt2_4_8Bytes(null)));
        }

        // save a reference to the big buffer we're reading from:
        GrowableByteBuffer bigBuf = buf;

        if (version < 7) {
            // In V6 and earlier, the length included the header.
            int headerSize = position() - startPos;
            dataSize -= headerSize;
        }
        byte[] destination = compressor.decompress(compression, getBuf().array(), position(), uncompressedSize, Optional.of(dataSize));

        // set position in original buffer to after data
        position(position() + dataSize);

        // for a while: deserialize from this buffer instead:
        buf = GrowableByteBuffer.wrap(destination);

        s.clear();
        StructDataType type = s.getDataType();
        for (int i=0; i<numberOfFields; ++i) {
            Field structField = type.getField(fieldIdsAndLengths.get(i).first, version);
            if (structField == null) {
                //ignoring unknown field:
                position(position() + fieldIdsAndLengths.get(i).second.intValue());
            } else {
                int posBefore = position();
                FieldValue value = structField.getDataType().createFieldValue();
                value.deserialize(structField, this);
                s.setFieldValue(structField, value);
                //jump to beginning of next field:
                position(posBefore + fieldIdsAndLengths.get(i).second.intValue());
            }
        }

        // restore the original buffer
        buf = bigBuf;
    }

    public void read(FieldBase field, StructuredFieldValue value) {
        throw new IllegalArgumentException("read not implemented yet.");
    }
    public <T extends FieldValue> void read(FieldBase field, WeightedSet<T> ws) {
        WeightedSetDataType type = ws.getDataType();
        getInt(null); // Have no need for type

        int numElements = getInt(null);
        if (numElements < 0) {
            throw new DeserializationException("Bad number of weighted set elements, " + numElements);
        }

        ws.clearAndReserve(numElements * 2); // Avoid resizing
        for (int i = 0; i < numElements; i++) {
            int size = getInt(null);
            FieldValue value = type.getNestedType().createFieldValue();
            value.deserialize(null, this);
            IntegerFieldValue weight = new IntegerFieldValue(getInt(null));
            ws.putUnChecked((T) value, weight);
        }

    }

    public void read(FieldBase field, AnnotationReference value) {
        int seqId = buf.getInt1_2_4Bytes();
        try {
            Annotation a = annotations.get(seqId);
            value.setReferenceNoCompatibilityCheck(a);
        } catch (IndexOutOfBoundsException iiobe) {
            throw new SerializationException("Could not serialize AnnotationReference value, reference not found.", iiobe);
        }
    }

    private Utf8String deserializeAttributeString() throws DeserializationException {
        int length = getByte(null);
        return new Utf8String(parseNullTerminatedString(length));
    }

    private Utf8Array parseNullTerminatedString() { return parseNullTerminatedString(getBuf().getByteBuffer()); }
    private Utf8Array parseNullTerminatedString(int lengthExcludingNull) { return parseNullTerminatedString(getBuf().getByteBuffer(), lengthExcludingNull); }

    static Utf8Array parseNullTerminatedString(ByteBuffer buf, int lengthExcludingNull) throws DeserializationException {
        Utf8Array utf8 = new Utf8Array(buf, lengthExcludingNull);
        buf.get();    //move past 0-termination
        return utf8;
    }

    static  Utf8Array parseNullTerminatedString(ByteBuffer buf) throws DeserializationException {
        //search for 0-byte
        int end = getFirstNullByte(buf);

        if (end == -1) {
            throw new DeserializationException("Could not locate terminating 0-byte for string");
        }

        return parseNullTerminatedString(buf, end - buf.position());
    }

    private static int getFirstNullByte(ByteBuffer buf) {
        int end = -1;
        int start = buf.position();

        while (true) {
            try {
                byte dataByte = buf.get();
                if (dataByte == (byte) 0) {
                    end = buf.position() - 1;
                    break;
                }
            } catch (Exception e) {
                break;
            }
        }

        buf.position(start);
        return end;
    }

    public void read(DocumentUpdate update) {
        short serializationVersion = getShort(null);

        update.setId(new DocumentId(this));

        byte contents = getByte(null);

        if ((contents & 0x1) == 0) {
            throw new DeserializationException("Cannot deserialize DocumentUpdate without doctype");
        }

        update.setDocumentType(readDocumentType());

        int size = getInt(null);

        for (int i = 0; i < size; i++) {
            // TODO: Should use checked method, but doesn't work according to test now.
            update.addFieldUpdateNoCheck(new FieldUpdate(this, update.getDocumentType(), serializationVersion));
        }
    }

    public void read(FieldPathUpdate update) {
        String fieldPath = getString(null);
        String whereClause = getString(null);
        update.setFieldPath(fieldPath);

        try {
            update.setWhereClause(whereClause);
        } catch (ParseException e) {
            throw new DeserializationException(e);
        }
    }

    public void read(AssignFieldPathUpdate update) {
        byte flags = getByte(null);
        update.setRemoveIfZero((flags & AssignFieldPathUpdate.REMOVE_IF_ZERO) != 0);
        update.setCreateMissingPath((flags & AssignFieldPathUpdate.CREATE_MISSING_PATH) != 0);
        if ((flags & AssignFieldPathUpdate.ARITHMETIC_EXPRESSION) != 0) {
            update.setExpression(getString(null));
        } else {
            DataType dt = update.getFieldPath().getResultingDataType();
            FieldValue fv = dt.createFieldValue();
            fv.deserialize(this);
            update.setNewValue(fv);
        }
    }

    public void read(RemoveFieldPathUpdate update) {

    }

    public void read(AddFieldPathUpdate update) {
        DataType dt = update.getFieldPath().getResultingDataType();
        FieldValue fv = dt.createFieldValue();
        dt.createFieldValue();
        fv.deserialize(this);

        if (!(fv instanceof Array)) {
            throw new DeserializationException("Add only applicable to array types");
        }
        update.setNewValues((Array)fv);
    }

    public ValueUpdate getValueUpdate(DataType superType, DataType subType) {
        int vuTypeId = getInt(null);

        ValueUpdate.ValueUpdateClassID op = ValueUpdate.ValueUpdateClassID.getID(vuTypeId);
        if (op == null) {
            throw new IllegalArgumentException("Read type "+vuTypeId+" of bytebuffer, but this is not a legal value update type.");
        }

        switch (op) {
            case ADD:
            {
                FieldValue fval = subType.createFieldValue();
                fval.deserialize(this);
                int weight = getInt(null);
                return new AddValueUpdate(fval, weight);
            }
            case ARITHMETIC:
                int opId = getInt(null);
                ArithmeticValueUpdate.Operator operator = ArithmeticValueUpdate.Operator.getID(opId);
                double operand = getDouble(null);
                return new ArithmeticValueUpdate(operator, operand);
            case ASSIGN:
            {
                byte contents = getByte(null);
                FieldValue fval = null;
                if (contents == (byte) 1) {
                    fval = superType.createFieldValue();
                    fval.deserialize(this);
                }
                return new AssignValueUpdate(fval);
            }
            case CLEAR:
                return new ClearValueUpdate();
            case MAP:
                if (superType instanceof ArrayDataType) {
                    CollectionDataType type = (CollectionDataType) superType;
                    IntegerFieldValue index = new IntegerFieldValue();
                    index.deserialize(this);
                    ValueUpdate update = getValueUpdate(type.getNestedType(), null);
                    return new MapValueUpdate(index, update);
                } else if (superType instanceof WeightedSetDataType) {
                    CollectionDataType type = (CollectionDataType) superType;
                    FieldValue fval = type.getNestedType().createFieldValue();
                    fval.deserialize(this);
                    ValueUpdate update = getValueUpdate(DataType.INT, null);
                    return new MapValueUpdate(fval, update);
                } else {
                    throw new DeserializationException("MapValueUpdate only works for arrays and weighted sets");
                }
            case REMOVE:
                FieldValue fval = ((CollectionDataType) superType).getNestedType().createFieldValue();
                fval.deserialize(this);
                return new RemoveValueUpdate(fval);
            default:
                throw new DeserializationException(
                        "Could not deserialize ValueUpdate, unknown valueUpdateClassID type " + vuTypeId);
        }
    }

    public void read(FieldUpdate fieldUpdate) {
        int fieldId = getInt(null);
        Field field = fieldUpdate.getDocumentType().getField(fieldId, fieldUpdate.getSerializationVersion());
        if (field == null) {
            throw new DeserializationException(
                    "Cannot deserialize FieldUpdate, field fieldId " + fieldId + " not found in " + fieldUpdate.getDocumentType());
        }

        fieldUpdate.setField(field);
        int size = getInt(null);

        for (int i = 0; i < size; i++) {
            if (field.getDataType() instanceof CollectionDataType) {
                CollectionDataType collType = (CollectionDataType) field.getDataType();
                fieldUpdate.addValueUpdate(getValueUpdate(collType, collType.getNestedType()));
            } else {
                fieldUpdate.addValueUpdate(getValueUpdate(field.getDataType(), null));
            }
        }
    }

    public DocumentId readDocumentId() {
        Utf8String uri = new Utf8String(parseNullTerminatedString(getBuf().getByteBuffer()));
        return DocumentId.createFromSerialized(uri.toString());
    }

    public DocumentType readDocumentType() {
        Utf8Array docTypeName = parseNullTerminatedString();
        int ignored = getShort(null); // used to hold the version

        DocumentType docType = manager.getDocumentType(new DataTypeName(docTypeName));
        if (docType == null) {
            throw new DeserializationException("No known document type with name " + 
                                               new Utf8String(docTypeName).toString());
        }
        return docType;
    }

    private SpanNode readSpanNode() {
        byte type = buf.get();
        buf.position(buf.position() - 1);

        SpanNode retval;
        if ((type & Span.ID) == Span.ID) {
            retval = new Span();
            if (spanNodes != null) {
                spanNodes.add(retval);
            }
            read((Span) retval);
        } else if ((type & SpanList.ID) == SpanList.ID) {
            retval = new SpanList();
            if (spanNodes != null) {
                spanNodes.add(retval);
            }
            read((SpanList) retval);
        } else if ((type & AlternateSpanList.ID) == AlternateSpanList.ID) {
            retval = new AlternateSpanList();
            if (spanNodes != null) {
                spanNodes.add(retval);
            }
            read((AlternateSpanList) retval);
        } else {
            throw new DeserializationException("Cannot read SpanNode of type " + type);
        }
        return retval;
    }

    private void readSpanTree(SpanTree tree, boolean readName) {
        //we don't support serialization of nested span trees:
        if (spanNodes != null || annotations != null) {
            throw new SerializationException("Deserialization of nested SpanTrees is not supported.");
        }

        //we're going to write a new SpanTree, create a new Map for nodes:
        spanNodes = new ArrayList<SpanNode>();
        annotations = new ArrayList<Annotation>();

        try {
            if (readName) {
                StringFieldValue treeName = new StringFieldValue();
                treeName.deserialize(this);
                tree.setName(treeName.getString());
            }

            SpanNode root = readSpanNode();
            tree.setRoot(root);

            int numAnnotations = buf.getInt1_2_4Bytes();

            for (int i = 0; i < numAnnotations; i++) {
                Annotation a = new Annotation();
                annotations.add(a);
            }
            for (int i = 0; i < numAnnotations; i++) {
                read(annotations.get(i));
            }
            for (Annotation a : annotations) {
                tree.annotate(a);
            }

            for (SpanNode node: spanNodes) {
                if (node instanceof Span) {
                    correctIndexes((Span) node);
                }
            }
        } finally {
            //we're done, let's set this to null to save memory and prevent madness:
            spanNodes = null;
            annotations = null;
        }
    }

    public void read(SpanTree tree) {
        readSpanTree(tree, true);
    }

    public void read(Annotation annotation) {
        int annotationTypeId = buf.getInt();
        AnnotationType type = manager.getAnnotationTypeRegistry().getType(annotationTypeId);

        if (type == null) {
            throw new DeserializationException("Cannot deserialize annotation of type " + annotationTypeId + " (unknown type)");
        }

        annotation.setType(type);

        byte features = buf.get();
        int length = buf.getInt1_2_4Bytes();

        if ((features & (byte) 1) == (byte) 1) {
            //we have a span node
            int spanNodeId = buf.getInt1_2_4Bytes();
            try {
                SpanNode node = spanNodes.get(spanNodeId);
                annotation.setSpanNode(node);
            } catch (IndexOutOfBoundsException ioobe) {
                throw new DeserializationException("Could not deserialize annotation, associated span node not found ", ioobe);
            }
        }
        if ((features & (byte) 2) == (byte) 2) {
            //we have a value:
            int dataTypeId = buf.getInt();

            //if this data type ID the same as the one in our config?
            if (dataTypeId != type.getDataType().getId()) {
                //not the same, but we will handle it gracefully, and just skip past the data:
                buf.position(buf.position() + length - 4);
            } else {
                FieldValue value = type.getDataType().createFieldValue();
                value.deserialize(this);
                annotation.setFieldValue(value);
            }
        }
    }

    public void read(Span span) {
        byte type = buf.get();
        if ((type & Span.ID) != Span.ID) {
            throw new DeserializationException("Cannot deserialize Span with type " + type);
        }
        span.setFrom(buf.getInt1_2_4Bytes());
        span.setLength(buf.getInt1_2_4Bytes());
    }

    private void correctIndexes(Span span) {
        if (stringPositions == null) {
            throw new DeserializationException("Cannot deserialize Span, no access to parent StringFieldValue.");
        }
        int fromIndex = stringPositions[span.getFrom()];
        int toIndex = stringPositions[span.getTo()];
        int length = toIndex - fromIndex;

        span.setFrom(fromIndex);
        span.setLength(length);
    }

    public void read(SpanList spanList) {
        byte type = buf.get();
        if ((type & SpanList.ID) != SpanList.ID) {
            throw new DeserializationException("Cannot deserialize SpanList with type " + type);
        }
        List<SpanNode> nodes = readSpanList(spanList);
        for (SpanNode node : nodes) {
            spanList.add(node);
        }
    }

    public void read(AlternateSpanList altSpanList) {
        byte type = buf.get();
        if ((type & AlternateSpanList.ID) != AlternateSpanList.ID) {
            throw new DeserializationException("Cannot deserialize AlternateSpanList with type " + type);
        }
        int numSubTrees = buf.getInt1_2_4Bytes();

        for (int i = 0; i < numSubTrees; i++) {
            double prob = buf.getDouble();
            List<SpanNode> list = readSpanList(altSpanList);

            if (i == 0) {
                for (SpanNode node : list) {
                    altSpanList.add(node);
                }
                altSpanList.setProbability(0, prob);
            } else {
                altSpanList.addChildren(i, list, prob);
            }
        }
    }

    private List<SpanNode> readSpanList(SpanNodeParent parent) {
        int size = buf.getInt1_2_4Bytes();
        List<SpanNode> spanList = new ArrayList<SpanNode>();
        for (int i = 0; i < size; i++) {
            spanList.add(readSpanNode());
        }
        return spanList;
    }

}
