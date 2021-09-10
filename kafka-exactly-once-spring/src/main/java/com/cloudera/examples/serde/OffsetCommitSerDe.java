package com.cloudera.examples.serde;

import com.cloudera.examples.generated.OffsetCommitKey;
import com.cloudera.examples.generated.OffsetCommitValue;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OffsetCommitSerDe {
    final static private Schema KEY_SCHEMA = OffsetCommitKey.getClassSchema();
    final static private Schema VALUE_SCHEMA = OffsetCommitValue.getClassSchema();

    public OffsetCommitSerDe() { }

    public OffsetCommitKey deserializeKey(byte[] data) {
        return deserialize(data, KEY_SCHEMA);
    }

    public OffsetCommitValue deserializeValue(byte[] data) {
        return deserialize(data, VALUE_SCHEMA);
    }

    public byte[] serializeKey(OffsetCommitKey data) {
        return serialize(data, KEY_SCHEMA);
    }

    public byte[] serializeValue(OffsetCommitValue data) {
        return serialize(data, VALUE_SCHEMA);
    }

    public <T extends SpecificRecord> T deserialize(byte[] data, Schema schema) {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {
            return readAvroRecord(stream, schema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T extends SpecificRecord> T readAvroRecord(InputStream stream, Schema readerSchema) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<>(readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        return datumReader.read(null, decoder);
    }

    public <T extends SpecificRecord> byte[] serialize(T obj, Schema schema) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            writeAvroRecord(stream, obj, schema);
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (stream != null)
                    stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private <T extends SpecificRecord> void writeAvroRecord(OutputStream stream, T obj, Schema writerSchema) throws IOException {
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(writerSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        datumWriter.write(obj, encoder);
        encoder.flush();
    }

}