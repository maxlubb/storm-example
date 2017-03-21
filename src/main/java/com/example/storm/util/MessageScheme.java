package com.example.storm.util;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by lubinbin on 15/12/29.
 */
public class MessageScheme implements Scheme {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageScheme.class);
    private static final long serialVersionUID = 1L;

    public List<Object> deserialize(byte[] ser) {
        try {
            String msg = new String(ser, "UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }
    public Fields getOutputFields() {
        return new Fields("MSG");
    }

    public List<Object> deserialize(ByteBuffer byteBuffer) {
        return null;
    }
}