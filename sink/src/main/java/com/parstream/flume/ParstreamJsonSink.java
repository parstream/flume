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
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.common.base.Preconditions;
import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;

public class ParstreamJsonSink extends AbstractParstreamSink {

    private static final Log LOGGER = LogFactory.getLog(ParstreamJsonSink.class);

    private JsonAdaptor _jsonAdaptor;
    private File _jsonConfFile;
    private JsonReader _jsonReader;

    @Override
    public void start() {
        super.start();

        try {
            _jsonAdaptor = new JsonAdaptor(_jsonConfFile, getColumnInfo());
        } catch (JsonAdaptorException | IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    @Override
    public void configure(final Context context) {
        super.configure(context);
    }

    @Override
    protected void checkPreconditions(Context context) {
        Preconditions.checkState(context.getString(CONF_JSON_INI) != null,
                "No Parstream JSON mapping file specified. Use configuration key: " + CONF_JSON_INI);

        _jsonConfFile = new File(context.getString(CONF_JSON_INI));
        Preconditions.checkState(_jsonConfFile.exists(), "Specified Parstream JSON mapping file does not exist: "
                + _jsonConfFile.getAbsolutePath());
    }

    protected void processEvent(final Event event) {
        byte[] eventBody = event.getBody();
        if (eventBody != null) {
            try {
                String decodedEvent = new String(eventBody, "UTF-8");
                _jsonReader = Json.createReader(new StringReader(decodedEvent));
                JsonObject jsonObj = _jsonReader.readObject();

                insertIntoParstream(_jsonAdaptor.convertJson(jsonObj));
            } catch (UnsupportedEncodingException e) {
                logError(LOGGER, e.getMessage(), e);
            } catch (JsonAdaptorException e) {
                logError(LOGGER, e.getMessage(), e);
            } catch (JsonParsingException e) {
                logError(LOGGER, e.getMessage(), e);
            } catch (JsonException e) {
                logError(LOGGER, e.getMessage(), e);
            } finally {
                if (_jsonReader != null) {
                    _jsonReader.close();
                }
            }
        } else {
            logInfo(LOGGER, "received event with null body");
        }
    }
}
