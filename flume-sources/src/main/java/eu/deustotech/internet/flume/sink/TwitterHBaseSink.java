package eu.deustotech.internet.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 11/03/15.
 */
public class TwitterHBaseSink extends AbstractSink implements Configurable {

    Configuration config;
    HTable table;
    String columnFamily;
    private static final Logger logger = LoggerFactory.getLogger(TwitterHBaseSink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        logger.info("Begin transaction");
        transaction.begin();
        event = channel.take();

        if (event != null) {
            String eventBody = new String(event.getBody());
            try {
                JSONObject jsonObject = new JSONObject(eventBody);
                String id = jsonObject.getString("id_str");
                logger.info("Creating row with id " + id);
                Put put = iterateJSONObject(jsonObject, new Put(Bytes.toBytes(id)), "");
                table.put(put);
            } catch (JSONException e) {
                transaction.rollback();
                transaction.close();
                e.printStackTrace();
                logger.error(e.getMessage());
            } catch (RetriesExhaustedWithDetailsException e) {
                transaction.rollback();
                transaction.close();
                e.printStackTrace();
                logger.error(e.getMessage());
            } catch (InterruptedIOException e) {
                transaction.rollback();
                transaction.close();
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        } else {
            result = Status.BACKOFF;
        }
        logger.info("Transaction commit");
        transaction.commit();
        transaction.close();
        return result;
    }

    // Im not sure about this method...
    private Put iterateJSONArray(JSONArray jsonArray, Put put, String parent) throws JSONException {
        for (int i = 0; i < jsonArray.length(); i++) {
            String qualifier = parent + ":" + i;
            Object object = jsonArray.get(i);
            if (object instanceof JSONObject) {
                iterateJSONObject((JSONObject) object, put, qualifier);
            } else if (object instanceof  JSONArray) {
                iterateJSONArray((JSONArray) object, put, qualifier);
            } else {
                String value = String.valueOf(object);
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
        }
        return put;
    }

    private Put iterateJSONObject(JSONObject jsonObject, Put put, String parent) throws JSONException {
        Iterator keys = jsonObject.keys();
        while(keys.hasNext()) {
            String key = (String) keys.next();
            String qualifier = key;
            if (!parent.equals("")) {
                qualifier = parent + ":" + key;
            }
            Object object = jsonObject.get(key);
            if (object instanceof JSONObject) {
                iterateJSONObject((JSONObject) object, put, qualifier);
            } else if (object instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) object;
                for (int i = 0; i < jsonArray.length(); i++) {
                    Object arrayObject = jsonArray.get(i);
                    if (arrayObject instanceof JSONObject) {
                        iterateJSONObject((JSONObject) arrayObject, put, qualifier + ":" + i);
                    } else if (arrayObject instanceof JSONArray) {
                        iterateJSONArray((JSONArray) arrayObject, put, qualifier + ":" + i);
                    } else {
                        String value = String.valueOf(arrayObject);
                        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                    }
                }
            }
            else {
                String value = String.valueOf(object);
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
        }
        return put;
    }

    @Override
    public void configure(Context context) {
        config = HBaseConfiguration.create();
        String tableName = context.getString(TwitterHBaseSinkConstants.HBASE_TABLE_NAME);
        try {
            table = new HTable(config, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        columnFamily = context.getString(TwitterHBaseSinkConstants.HBASE_COLUMN_FAMILY);
    }
}
