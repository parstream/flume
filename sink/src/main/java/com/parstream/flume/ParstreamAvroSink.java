/**
 * Copyright 2015 ParStream GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.parstream.flume;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.common.base.Preconditions;
import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.adaptor.avro.AvroAdaptorException;

public class ParstreamAvroSink extends AbstractParstreamSink {

    private static final Log LOGGER = LogFactory.getLog(ParstreamAvroSink.class);

    private AvroAdaptor _avroAdaptor;
    private File _avroConfFile;

    private Schema _avroSchema;
    private GenericDatumReader<GenericRecord> _avroReader;

    private BinaryDecoder _binaryDecoder;
    private GenericRecord _decodedRecord;
    private DecoderFactory _decoderFactory;

    @Override
    public void start() {
        super.start();

        try {
            _avroAdaptor = new AvroAdaptor(_avroConfFile, getColumnInfo());
        } catch (AvroAdaptorException | IOException e) {
            throw new IllegalStateException(e.getMessage());
        }

        _decoderFactory = new DecoderFactory();
    }

    @Override
    public void configure(final Context context) {
        super.configure(context);
    }

    @Override
    protected void checkPreconditions(Context context) {
        Preconditions.checkState(context.getString(CONF_AVRO_INI) != null,
                "No Parstream Avro mapping file specified. Use configuration key: " + CONF_AVRO_INI);

        _avroConfFile = new File(context.getString(CONF_AVRO_INI));
        Preconditions.checkState(_avroConfFile.exists(), "Specified Parstream Avro mapping file does not exist: "
                + _avroConfFile.getAbsolutePath());
    }

    protected void processEvent(final Event event) {
        try {
            if (_avroSchema == null) {
                Map<String, String> eventHeaders = event.getHeaders();
                String schemaString = eventHeaders.get("flume.avro.schema.literal");
                _avroSchema = new Schema.Parser().parse(schemaString);
                _avroReader = new GenericDatumReader<GenericRecord>(_avroSchema);
            }
            _binaryDecoder = _decoderFactory.binaryDecoder(event.getBody(), _binaryDecoder);
            _decodedRecord = _avroReader.read(_decodedRecord, _binaryDecoder);
            insertIntoParstream(_avroAdaptor.convertRecord(_decodedRecord));
        } catch (AvroAdaptorException | IOException e) {
            logError(LOGGER, e.getMessage(), e);
        }
    }
}
