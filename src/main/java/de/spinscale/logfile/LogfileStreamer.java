package de.spinscale.logfile;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;

/*
curl -X PUT localhost:9200/logfile
curl -X PUT localhost:9200/logfile/log/_mapping -d '
{
    "log" : {
        "properties" : {
            "geohash" : {
                "type" : "geo_point",
                "geohash": true
            }
        }
    }
}'
 */
public class LogfileStreamer {

    private static final String INDEX = "logfile";
    private static final String TYPE = "log";
    private static final int MAX_BULK_SIZE = 100;
    private final TransportClient client;
    private BulkRequestBuilder bulk;
    private StopWatch sw;
    private long startTimestamp;

    public static void main(String[] args) throws Exception {
        new LogfileStreamer().run();
    }

    public LogfileStreamer() {
        client = new TransportClient().addTransportAddress(new
                InetSocketTransportAddress("localhost", 9300));
        reset();
    }

    public void run() throws Exception {
        ObjectReader reader = new ObjectMapper().reader(Map.class);
        MappingIterator<Map<String, Object>> iterator = reader.readValues(getInputStream());

        try {
            while (iterator.hasNextValue()) {
                Map<String, Object> entry = iterator.nextValue();
                if (entry.containsKey("_heartbeat_")) continue;

                if (entry.containsKey("ll") && entry.containsKey("t")) {
                    List<Double> location = (List<Double>) entry.get("ll");
                    long timestamp = ((Integer) entry.get("t")).longValue();
                    double latitude = location.get(0);
                    double longitude = location.get(1);

                    addToBulkRequest(timestamp, latitude, longitude);
                }
            }
        } finally {
            executeBulkRequest();
        }
    }

    private void addToBulkRequest(long timestamp, double latitude, double longitude) {
        String geohash = GeoHashUtils.encode(latitude, longitude);
        String json = String.format("{\"timestamp\":\"%s\", \"geohash\":\"%s\" }", timestamp, geohash);
        bulk.add(indexRequest().index(INDEX).type(TYPE).source(json));
        System.out.print(".");

        executeBulkRequest();
    }

    private void executeBulkRequest() {
        if (bulk.numberOfActions() == 0) return;
        long secondsSinceLastUpdate = System.currentTimeMillis() / 1000 - startTimestamp;
        if (bulk.numberOfActions() < MAX_BULK_SIZE && secondsSinceLastUpdate < 10) return;

        logStatistics(bulk.execute().actionGet().getItems().length);
        reset();
    }

    private void logStatistics(long itemsIndexed) {
        long totalTimeInSeconds = sw.stop().totalTime().seconds();
        double totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? itemsIndexed: (double) itemsIndexed / totalTimeInSeconds;
        System.out.println(String.format("\nIndexed %s documents, %.2f per second in %s seconds", itemsIndexed, totalDocumentsPerSecond, totalTimeInSeconds));
    }

    private void reset() {
        sw = new StopWatch().start();
        startTimestamp = System.currentTimeMillis() / 1000;
        bulk = client.prepareBulk();
    }

    private InputStream getInputStream() throws Exception {
        URL url = new URL("http://developer.usa.gov/1usagov");
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        return request.getInputStream();
    }
}
