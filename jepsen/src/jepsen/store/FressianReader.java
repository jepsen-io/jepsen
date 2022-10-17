//   Copyright (c) Metadata Partners, LLC, with modifications by Jepsen, LLC.
//   All rights reserved. The use and distribution terms for this software are
//   covered by the Eclipse Public License 1.0
//   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the
//   file epl-v10.html at the root of this distribution.
//   By using this software in any fashion, you are agreeing to be bound by
//   the terms of this license. You must not remove this notice, or any other,
//   from this software.

// This is a very straightforward patch to the original
// (https://github.com/Datomic/fressian/blob/43e34cade9b7b498fa88db1dd88546ff07f2282a/src/org/fressian/FressianReader.java)
// which modifies the coreHandlers map to construct vectors rather than
// arraylists. If Fressian ever makes this configurable (it's sooooo close
// already!) we can get rid of this file.

package jepsen.store;

import clojure.lang.PersistentVector;

import org.fressian.*;
import org.fressian.handlers.*;
import org.fressian.impl.*;

import java.io.*;
import java.util.*;

import static org.fressian.impl.Fns.*;

public class FressianReader implements org.fressian.Reader, Closeable {
    private final RawInput is;
    private ArrayList<Object> priorityCache;
    private ArrayList<Object> structCache;
    public final Map standardExtensionHandlers;
    private final ILookup<Object, ReadHandler> handlerLookup;
    private byte[] byteBuffer;

    public FressianReader(InputStream is) {
        this(is, null, true);
    }

    public FressianReader(InputStream is, ILookup<Object, ReadHandler> handlerLookup) {
        this(is, handlerLookup, true);
    }

    public FressianReader(InputStream is, ILookup<Object, ReadHandler> handlerLookup, boolean validateAdler) {
        standardExtensionHandlers = Handlers.extendedReadHandlers;
        this.is = new RawInput(is, validateAdler);
        this.handlerLookup = handlerLookup;
        resetCaches();
    }

    public boolean readBoolean() throws IOException {
        int code = readNextCode();

        switch (code) {
            case Codes.TRUE:
                return true;
            case Codes.FALSE:
                return false;
            default: {
                Object result = read(code);
                if (result instanceof Boolean) {
                    return (Boolean) result;
                } else {
                    throw expected("boolean", code, result);
                }
            }
        }
    }

    public long readInt() throws IOException {
        return internalReadInt();
    }

    private long internalReadInt() throws IOException {
        long result;
        int code = readNextCode();
        switch (code) {

            //INT_PACKED_1_FIRST
            case 0xFF:
                result = -1;
                break;

            case 0x00:
            case 0x01:
            case 0x02:
            case 0x03:
            case 0x04:
            case 0x05:
            case 0x06:
            case 0x07:
            case 0x08:
            case 0x09:
            case 0x0A:
            case 0x0B:
            case 0x0C:
            case 0x0D:
            case 0x0E:
            case 0x0F:
            case 0x10:
            case 0x11:
            case 0x12:
            case 0x13:
            case 0x14:
            case 0x15:
            case 0x16:
            case 0x17:
            case 0x18:
            case 0x19:
            case 0x1A:
            case 0x1B:
            case 0x1C:
            case 0x1D:
            case 0x1E:
            case 0x1F:
            case 0x20:
            case 0x21:
            case 0x22:
            case 0x23:
            case 0x24:
            case 0x25:
            case 0x26:
            case 0x27:
            case 0x28:
            case 0x29:
            case 0x2A:
            case 0x2B:
            case 0x2C:
            case 0x2D:
            case 0x2E:
            case 0x2F:
            case 0x30:
            case 0x31:
            case 0x32:
            case 0x33:
            case 0x34:
            case 0x35:
            case 0x36:
            case 0x37:
            case 0x38:
            case 0x39:
            case 0x3A:
            case 0x3B:
            case 0x3C:
            case 0x3D:
            case 0x3E:
            case 0x3F:
                result = (long) code & 0xff;
                break;

//  INT_PACKED_2_FIRST
            case 0x40:
            case 0x41:
            case 0x42:
            case 0x43:
            case 0x44:
            case 0x45:
            case 0x46:
            case 0x47:
            case 0x48:
            case 0x49:
            case 0x4A:
            case 0x4B:
            case 0x4C:
            case 0x4D:
            case 0x4E:
            case 0x4F:
            case 0x50:
            case 0x51:
            case 0x52:
            case 0x53:
            case 0x54:
            case 0x55:
            case 0x56:
            case 0x57:
            case 0x58:
            case 0x59:
            case 0x5A:
            case 0x5B:
            case 0x5C:
            case 0x5D:
            case 0x5E:
            case 0x5F:
                result = ((long) (code - Codes.INT_PACKED_2_ZERO) << 8) | is.readRawInt8();
                break;

//  INT_PACKED_3_FIRST
            case 0x60:
            case 0x61:
            case 0x62:
            case 0x63:
            case 0x64:
            case 0x65:
            case 0x66:
            case 0x67:
            case 0x68:
            case 0x69:
            case 0x6A:
            case 0x6B:
            case 0x6C:
            case 0x6D:
            case 0x6E:
            case 0x6F:
                result = ((long) (code - Codes.INT_PACKED_3_ZERO) << 16) | is.readRawInt16();
                break;


//  INT_PACKED_4_FIRST
            case 0x70:
            case 0x71:
            case 0x72:
            case 0x73:
                result = ((long) (code - Codes.INT_PACKED_4_ZERO << 24)) | is.readRawInt24();
                break;


//  INT_PACKED_5_FIRST
            case 0x74:
            case 0x75:
            case 0x76:
            case 0x77:
                result = ((long) (code - Codes.INT_PACKED_5_ZERO) << 32) | is.readRawInt32();
                break;

//  INT_PACKED_6_FIRST
            case 0x78:
            case 0x79:
            case 0x7A:
            case 0x7B:
                result = (((long) code - Codes.INT_PACKED_6_ZERO) << 40) | is.readRawInt40();
                break;

//  INT_PACKED_7_FIRST
            case 0x7C:
            case 0x7D:
            case 0x7E:
            case 0x7F:
                result = (((long) code - Codes.INT_PACKED_7_ZERO) << 48) | is.readRawInt48();
                break;

            case Codes.INT:
                result = is.readRawInt64();
                break;

            default: {
                Object o = read(code);
                if (o instanceof Long) {
                    return (Long) o;
                } else {
                    throw expected("int64", code, o);
                }
            }
        }
        return result;
    }

    public double readDouble() throws IOException {
        int code = readNextCode();
        double d = internalReadDouble(code);
        return d;
    }

    public float readFloat() throws IOException {
        int code = readNextCode();
        float result;
        switch (code) {
            case Codes.FLOAT:
                result = is.readRawFloat();
                break;
            default: {
                Object o = read(code);
                if (o instanceof Float) {
                    return (Float) o;
                } else {
                    throw expected("float", code, o);
                }
            }
        }
        return result;
    }

    public Object readObject() throws IOException {
        return read(readNextCode());
    }

    private Object read(int code) throws IOException {
        Object result;
        switch (code) {

            //INT_PACKED_1_FIRST
            case 0xFF:
                result = -1L;
                break;

            case 0x00:
            case 0x01:
            case 0x02:
            case 0x03:
            case 0x04:
            case 0x05:
            case 0x06:
            case 0x07:
            case 0x08:
            case 0x09:
            case 0x0A:
            case 0x0B:
            case 0x0C:
            case 0x0D:
            case 0x0E:
            case 0x0F:
            case 0x10:
            case 0x11:
            case 0x12:
            case 0x13:
            case 0x14:
            case 0x15:
            case 0x16:
            case 0x17:
            case 0x18:
            case 0x19:
            case 0x1A:
            case 0x1B:
            case 0x1C:
            case 0x1D:
            case 0x1E:
            case 0x1F:
            case 0x20:
            case 0x21:
            case 0x22:
            case 0x23:
            case 0x24:
            case 0x25:
            case 0x26:
            case 0x27:
            case 0x28:
            case 0x29:
            case 0x2A:
            case 0x2B:
            case 0x2C:
            case 0x2D:
            case 0x2E:
            case 0x2F:
            case 0x30:
            case 0x31:
            case 0x32:
            case 0x33:
            case 0x34:
            case 0x35:
            case 0x36:
            case 0x37:
            case 0x38:
            case 0x39:
            case 0x3A:
            case 0x3B:
            case 0x3C:
            case 0x3D:
            case 0x3E:
            case 0x3F:
                result = (long) code & 0xff;
                break;

//  INT_PACKED_2_FIRST
            case 0x40:
            case 0x41:
            case 0x42:
            case 0x43:
            case 0x44:
            case 0x45:
            case 0x46:
            case 0x47:
            case 0x48:
            case 0x49:
            case 0x4A:
            case 0x4B:
            case 0x4C:
            case 0x4D:
            case 0x4E:
            case 0x4F:
            case 0x50:
            case 0x51:
            case 0x52:
            case 0x53:
            case 0x54:
            case 0x55:
            case 0x56:
            case 0x57:
            case 0x58:
            case 0x59:
            case 0x5A:
            case 0x5B:
            case 0x5C:
            case 0x5D:
            case 0x5E:
            case 0x5F:
                result = ((long) (code - Codes.INT_PACKED_2_ZERO) << 8) | is.readRawInt8();
                break;

//  INT_PACKED_3_FIRST
            case 0x60:
            case 0x61:
            case 0x62:
            case 0x63:
            case 0x64:
            case 0x65:
            case 0x66:
            case 0x67:
            case 0x68:
            case 0x69:
            case 0x6A:
            case 0x6B:
            case 0x6C:
            case 0x6D:
            case 0x6E:
            case 0x6F:
                result = ((long) (code - Codes.INT_PACKED_3_ZERO) << 16) | is.readRawInt16();
                break;

//  INT_PACKED_4_FIRST
            case 0x70:
            case 0x71:
            case 0x72:
            case 0x73:
                result = ((long) (code - Codes.INT_PACKED_4_ZERO << 24)) | is.readRawInt24();
                break;

//  INT_PACKED_5_FIRST
            case 0x74:
            case 0x75:
            case 0x76:
            case 0x77:
                result = ((long) (code - Codes.INT_PACKED_5_ZERO) << 32) | is.readRawInt32();
                break;

//  INT_PACKED_6_FIRST
            case 0x78:
            case 0x79:
            case 0x7A:
            case 0x7B:
                result = (((long) code - Codes.INT_PACKED_6_ZERO) << 40) | is.readRawInt40();
                break;

//  INT_PACKED_7_FIRST
            case 0x7C:
            case 0x7D:
            case 0x7E:
            case 0x7F:
                result = (((long) code - Codes.INT_PACKED_7_ZERO) << 48) | is.readRawInt48();
                break;

            case Codes.PUT_PRIORITY_CACHE:
                result = readAndCacheObject(getPriorityCache());
                break;

            case Codes.GET_PRIORITY_CACHE:
                result = lookupCache(getPriorityCache(), readInt32());
                break;

            case Codes.PRIORITY_CACHE_PACKED_START + 0:
            case Codes.PRIORITY_CACHE_PACKED_START + 1:
            case Codes.PRIORITY_CACHE_PACKED_START + 2:
            case Codes.PRIORITY_CACHE_PACKED_START + 3:
            case Codes.PRIORITY_CACHE_PACKED_START + 4:
            case Codes.PRIORITY_CACHE_PACKED_START + 5:
            case Codes.PRIORITY_CACHE_PACKED_START + 6:
            case Codes.PRIORITY_CACHE_PACKED_START + 7:
            case Codes.PRIORITY_CACHE_PACKED_START + 8:
            case Codes.PRIORITY_CACHE_PACKED_START + 9:
            case Codes.PRIORITY_CACHE_PACKED_START + 10:
            case Codes.PRIORITY_CACHE_PACKED_START + 11:
            case Codes.PRIORITY_CACHE_PACKED_START + 12:
            case Codes.PRIORITY_CACHE_PACKED_START + 13:
            case Codes.PRIORITY_CACHE_PACKED_START + 14:
            case Codes.PRIORITY_CACHE_PACKED_START + 15:
            case Codes.PRIORITY_CACHE_PACKED_START + 16:
            case Codes.PRIORITY_CACHE_PACKED_START + 17:
            case Codes.PRIORITY_CACHE_PACKED_START + 18:
            case Codes.PRIORITY_CACHE_PACKED_START + 19:
            case Codes.PRIORITY_CACHE_PACKED_START + 20:
            case Codes.PRIORITY_CACHE_PACKED_START + 21:
            case Codes.PRIORITY_CACHE_PACKED_START + 22:
            case Codes.PRIORITY_CACHE_PACKED_START + 23:
            case Codes.PRIORITY_CACHE_PACKED_START + 24:
            case Codes.PRIORITY_CACHE_PACKED_START + 25:
            case Codes.PRIORITY_CACHE_PACKED_START + 26:
            case Codes.PRIORITY_CACHE_PACKED_START + 27:
            case Codes.PRIORITY_CACHE_PACKED_START + 28:
            case Codes.PRIORITY_CACHE_PACKED_START + 29:
            case Codes.PRIORITY_CACHE_PACKED_START + 30:
            case Codes.PRIORITY_CACHE_PACKED_START + 31:
                result = lookupCache(getPriorityCache(), code - Codes.PRIORITY_CACHE_PACKED_START);
                break;

            case Codes.STRUCT_CACHE_PACKED_START + 0:
            case Codes.STRUCT_CACHE_PACKED_START + 1:
            case Codes.STRUCT_CACHE_PACKED_START + 2:
            case Codes.STRUCT_CACHE_PACKED_START + 3:
            case Codes.STRUCT_CACHE_PACKED_START + 4:
            case Codes.STRUCT_CACHE_PACKED_START + 5:
            case Codes.STRUCT_CACHE_PACKED_START + 6:
            case Codes.STRUCT_CACHE_PACKED_START + 7:
            case Codes.STRUCT_CACHE_PACKED_START + 8:
            case Codes.STRUCT_CACHE_PACKED_START + 9:
            case Codes.STRUCT_CACHE_PACKED_START + 10:
            case Codes.STRUCT_CACHE_PACKED_START + 11:
            case Codes.STRUCT_CACHE_PACKED_START + 12:
            case Codes.STRUCT_CACHE_PACKED_START + 13:
            case Codes.STRUCT_CACHE_PACKED_START + 14:
            case Codes.STRUCT_CACHE_PACKED_START + 15: {
                StructType st = (StructType) lookupCache(getStructCache(), code - Codes.STRUCT_CACHE_PACKED_START);
                result = handleStruct(st.tag, st.fields);
                break;
            }

            case Codes.MAP:
                result = handleStruct("map", 1);
                break;

            case Codes.SET:
                result = handleStruct("set", 1);
                break;

            case Codes.UUID:
                result = handleStruct("uuid", 2);
                break;

            case Codes.REGEX:
                result = handleStruct("regex", 1);
                break;

            case Codes.URI:
                result = handleStruct("uri", 1);
                break;

            case Codes.BIGINT:
                result = handleStruct("bigint", 1);
                break;

            case Codes.BIGDEC:
                result = handleStruct("bigdec", 2);
                break;

            case Codes.INST:
                result = handleStruct("inst", 1);
                break;

            case Codes.SYM:
                result = handleStruct("sym", 2);
                break;

            case Codes.KEY:
                result = handleStruct("key", 2);
                break;

            case Codes.INT_ARRAY:
                result = handleStruct("int[]", 2);
                break;

            case Codes.LONG_ARRAY:
                result = handleStruct("long[]", 2);
                break;

            case Codes.FLOAT_ARRAY:
                result = handleStruct("float[]", 2);
                break;

            case Codes.BOOLEAN_ARRAY:
                result = handleStruct("boolean[]", 2);
                break;

            case Codes.DOUBLE_ARRAY:
                result = handleStruct("double[]", 2);
                break;

            case Codes.OBJECT_ARRAY:
                result = handleStruct("Object[]", 2);
                break;

            case Codes.BYTES_PACKED_LENGTH_START + 0:
            case Codes.BYTES_PACKED_LENGTH_START + 1:
            case Codes.BYTES_PACKED_LENGTH_START + 2:
            case Codes.BYTES_PACKED_LENGTH_START + 3:
            case Codes.BYTES_PACKED_LENGTH_START + 4:
            case Codes.BYTES_PACKED_LENGTH_START + 5:
            case Codes.BYTES_PACKED_LENGTH_START + 6:
            case Codes.BYTES_PACKED_LENGTH_START + 7:
                result = internalReadBytes(code - Codes.BYTES_PACKED_LENGTH_START);
                break;

            case Codes.BYTES:
                result = internalReadBytes(readCount());
                break;

            case Codes.BYTES_CHUNK:
                result = internalReadChunkedBytes();
                break;

            case Codes.STRING_PACKED_LENGTH_START + 0:
            case Codes.STRING_PACKED_LENGTH_START + 1:
            case Codes.STRING_PACKED_LENGTH_START + 2:
            case Codes.STRING_PACKED_LENGTH_START + 3:
            case Codes.STRING_PACKED_LENGTH_START + 4:
            case Codes.STRING_PACKED_LENGTH_START + 5:
            case Codes.STRING_PACKED_LENGTH_START + 6:
            case Codes.STRING_PACKED_LENGTH_START + 7:
                result = internalReadString(code - Codes.STRING_PACKED_LENGTH_START).toString();
                break;

            case Codes.STRING:
                result = internalReadString(readCount()).toString();
                break;

            case Codes.STRING_CHUNK:
                result = internalReadChunkedString(readCount());
                break;

            case Codes.LIST_PACKED_LENGTH_START + 0:
            case Codes.LIST_PACKED_LENGTH_START + 1:
            case Codes.LIST_PACKED_LENGTH_START + 2:
            case Codes.LIST_PACKED_LENGTH_START + 3:
            case Codes.LIST_PACKED_LENGTH_START + 4:
            case Codes.LIST_PACKED_LENGTH_START + 5:
            case Codes.LIST_PACKED_LENGTH_START + 6:
            case Codes.LIST_PACKED_LENGTH_START + 7:
                result = internalReadList(code - Codes.LIST_PACKED_LENGTH_START);
                break;

            case Codes.LIST:
                result = internalReadList(readCount());
                break;

            case Codes.BEGIN_CLOSED_LIST:
                result = ((ConvertList) getHandler("list")).convertList(readClosedList());
                break;

            case Codes.BEGIN_OPEN_LIST:
                result = ((ConvertList) getHandler("list")).convertList(readOpenList());
                break;

            case Codes.TRUE:
                result = Boolean.TRUE;
                break;

            case Codes.FALSE:
                result = Boolean.FALSE;
                break;

            case Codes.DOUBLE:
            case Codes.DOUBLE_0:
            case Codes.DOUBLE_1:
                result = ((ConvertDouble) getHandler("double")).convertDouble(internalReadDouble(code));
                break;

            case Codes.FLOAT:
                result = ((ConvertFloat) getHandler("float")).convertFloat(is.readRawFloat());
                break;

            case Codes.INT:
                result = is.readRawInt64();
                break;

            case Codes.NULL:
                result = null;
                break;

            case Codes.FOOTER: {
                int calculatedLength = is.getBytesRead() - 1;
                int magicFromStream = (int) ((code << 24) + (int) is.readRawInt24());
                validateFooter(calculatedLength, magicFromStream);
                return readObject();
            }
            case Codes.STRUCTTYPE: {
                Object tag = readObject();
                int fields = readInt32();
                getStructCache().add(new StructType(tag, fields));
                result = handleStruct(tag, fields);
                break;
            }
            case Codes.STRUCT: {
                StructType st = (StructType) lookupCache(getStructCache(), readInt32());
                result = handleStruct(st.tag, st.fields);
                break;
            }

            case Codes.RESET_CACHES: {
                resetCaches();
                result = readObject();
                break;
            }


            default:
                throw expected("any", code);
        }
        return result;
    }

    private Object handleStruct(Object tag, int fields) throws IOException {
        ReadHandler h = lookup(handlerLookup, tag);
        if (h == null)
            h = (ReadHandler) standardExtensionHandlers.get(tag);
        if (h == null)
            return new TaggedObject(tag, readObjects(fields));
        else
            return h.read(this, tag, fields);
    }

    private int readCount() throws IOException {
        return readInt32();
    }

    private int internalReadInt32() throws IOException {
        return intCast(internalReadInt());
    }

    private int readInt32() throws IOException {
        return intCast(readInt());
    }

    private StringBuffer internalReadString(int length) throws IOException {
        return internalReadStringBuffer(new StringBuffer(length), length);
    }

    private StringBuffer internalReadStringBuffer(StringBuffer buf, int length) throws IOException {
        if ((byteBuffer == null) || (byteBuffer.length < length))
            byteBuffer = new byte[length];
        is.readFully(byteBuffer, 0, length);
        readUTF8Chars(buf, byteBuffer, 0, length);
        return buf;
    }

    private String internalReadChunkedString(int length) throws IOException {
        StringBuffer buf = internalReadString(length);
        boolean done = false;
        while (!done) {
            int code = readNextCode();
            switch (code) {
                case Codes.STRING_PACKED_LENGTH_START + 0:
                case Codes.STRING_PACKED_LENGTH_START + 1:
                case Codes.STRING_PACKED_LENGTH_START + 2:
                case Codes.STRING_PACKED_LENGTH_START + 3:
                case Codes.STRING_PACKED_LENGTH_START + 4:
                case Codes.STRING_PACKED_LENGTH_START + 5:
                case Codes.STRING_PACKED_LENGTH_START + 6:
                case Codes.STRING_PACKED_LENGTH_START + 7:
                    internalReadStringBuffer(buf, code - Codes.STRING_PACKED_LENGTH_START).toString();
                    done = true;
                    break;

                case Codes.STRING:
                    internalReadStringBuffer(buf, readCount());
                    done = true;
                    break;

                case Codes.STRING_CHUNK:
                    internalReadStringBuffer(buf, readCount());
                    break;
                default:
                    throw expected("chunked string", code);
            }
        }
        return buf.toString();
    }

    private byte[] internalReadBytes(int length) throws IOException {
        byte[] result = new byte[length];
        is.readFully(result, 0, length);
        return result;
    }

    private byte[] internalReadChunkedBytes() throws IOException {
        ArrayList<byte[]> chunks = new ArrayList<byte[]>();
        int code = Codes.BYTES_CHUNK;
        while (code == Codes.BYTES_CHUNK) {
            chunks.add(internalReadBytes(readCount()));
            code = readNextCode();
        }
        if (code != Codes.BYTES) {
            throw expected("conclusion of chunked bytes", code);
        }
        chunks.add(internalReadBytes(readCount()));
        int length = 0;
        for (int n=0; n < chunks.size(); n++) {
            length = length + chunks.get(n).length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (int n=0; n < chunks.size(); n++) {
            System.arraycopy(chunks.get(n), 0, result, pos, chunks.get(n).length);
            pos += chunks.get(n).length;
        }
        return result;
    }

    private Object getHandler(String tag) {
        Object o = coreHandlers.get(tag);
        if (o == null) {
            throw new RuntimeException("No read handler for type " + tag);
        }
        return o;
    }

    private double internalReadDouble(int code) throws IOException {
        switch (code) {
            case Codes.DOUBLE:
                return is.readRawDouble();
            case Codes.DOUBLE_0:
                return 0.0D;
            case Codes.DOUBLE_1:
                return 1.0D;
            default: {
                Object o = read(code);
                if (o instanceof Double) {
                    return (Double) o;
                } else {
                    throw expected("double", code, o);
                }
            }
        }
    }

    private Object[] readObjects(int length) throws IOException {
        Object[] objects = new Object[length];
        for (int n = 0; n < length; n++) {
            objects[n] = readObject();
        }
        return objects;
    }

    private Object[] readClosedList() throws IOException {
        ArrayList<Object> objects = new ArrayList<Object>();
        while (true) {
            int code = readNextCode();
            if (code == Codes.END_COLLECTION) {
                return objects.toArray();
            }
            objects.add(read(code));
        }
    }

    private Object[] readOpenList() throws IOException {
        ArrayList<Object> objects = new ArrayList<Object>();
        int code;
        while (true) {
            try {
                code = readNextCode();
            } catch (EOFException e) {
                code = Codes.END_COLLECTION;
            }
            if (code == Codes.END_COLLECTION) {
                return objects.toArray();
            }
            objects.add(read(code));
        }
    }

    public void close() throws IOException {
        is.close();
    }

    static class MapEntry implements Map.Entry {
        public final Object key;
        public final Object value;

        public MapEntry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Object setValue(Object o) {
            throw new UnsupportedOperationException();
        }
    }

    // placeholder for objects still being read in
    static private Object UNDER_CONSTRUCTION = new Object();

    private Object lookupCache(ArrayList cache, int index) {
        if (index < cache.size()) {
            Object result = cache.get(index);
            if (result == UNDER_CONSTRUCTION)
                throw new RuntimeException("Unable to resolve circular reference in cache");
            else
                return result;
        } else {
            throw new RuntimeException("Requested object beyond end of cache at " + index);
        }
    }

    private List internalReadList(int length) throws IOException {
        return ((ConvertList) getHandler("list")).convertList(readObjects(length));
    }

    private void validateFooter(int calculatedLength, int magicFromStream) throws IOException {
        if (magicFromStream != Codes.FOOTER_MAGIC) {
            throw new RuntimeException(String.format("Invalid footer magic, expected %X got %X", Codes.FOOTER_MAGIC, magicFromStream));
        }
        int lengthFromStream = (int) is.readRawInt32();
        if (lengthFromStream != calculatedLength) {
            throw new RuntimeException(String.format("Invalid footer length, expected %X got %X", calculatedLength, lengthFromStream));
        }
        is.validateChecksum();
        is.reset();
        resetCaches();
    }

    private ArrayList<Object> getPriorityCache() {
        if (priorityCache == null) priorityCache = new ArrayList<Object>();
        return priorityCache;
    }

    private ArrayList<Object> getStructCache() {
        if (structCache == null) structCache = new ArrayList<Object>();
        return structCache;
    }

    private void resetCaches() {
        if (priorityCache != null) priorityCache.clear();
        if (structCache != null) structCache.clear();
    }

    public void validateFooter() throws IOException {
        int calculatedLength = is.getBytesRead();
        int magicFromStream = (int) is.readRawInt32();
        validateFooter(calculatedLength, magicFromStream);
    }

    private int readNextCode() throws IOException {
        return is.readRawByte();
    }

    private Object readAndCacheObject(ArrayList<Object> cache) throws IOException {
        int index = cache.size();
        cache.add(UNDER_CONSTRUCTION);
        Object o = readObject();
        cache.set(index, o);
        return o;
    }

    public static final Map coreHandlers;

    static {
        HashMap<String, Object> handlers = new HashMap<String, Object>();
        handlers.put("list", new ConvertList() {
            public List convertList(Object[] items) {
              return PersistentVector.create(items);
            }
        });

        handlers.put("bytes", new ConvertBytes() {
            public Object convertBytes(byte[] bytes) {
                return bytes;
            }
        });

        handlers.put("double", new ConvertDouble() {
            public Object convertDouble(double d) {
                return Double.valueOf(d);
            }
        });

        handlers.put("float", new ConvertFloat() {
            public Object convertFloat(float f) {
                return Float.valueOf(f);
            }
        });
        coreHandlers = Collections.unmodifiableMap(handlers);
    }

}
