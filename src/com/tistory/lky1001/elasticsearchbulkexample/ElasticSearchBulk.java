package com.tistory.lky1001.elasticsearchbulkexample;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kylee on 2015. 12. 3..
 */
public class ElasticSearchBulk {

  private static final String ES_CLUSTER_NAME = "my_cluster";
  private static final String ES_HOST = "localhost";
  private static final int ES_PORT = 9300;
  private static final String AWS_ACCESS_KEY = "access key";
  private static final String AWS_SECRET_KEY = "secret key";
  private static final String AWS_RAW_DATA_BUCKET = "log_bucket";

  private static final String[] LOG_FILE_NAMES = {
      "user.log"
  };

  private ObjectMapper mapper = new ObjectMapper();

  private AmazonS3 s3Client;

  public static void main(String[] args) {
    ElasticSearchBulk elasticSearchBulk = new ElasticSearchBulk();
    elasticSearchBulk.start();
  }

  private void start() {
    setAwsConfig();
    Client client = getElasticSearchClient();

    Calendar calendar = Calendar.getInstance();

    calendar.add(Calendar.DAY_OF_MONTH, -1);

    SimpleDateFormat keyFormat = new SimpleDateFormat("yyyyMMdd");

    String keyDate = keyFormat.format(calendar.getTime());

    int len = LOG_FILE_NAMES.length;

    for (int i = 0; i < len; i++) {
      // 어제 로그
      String fileKey = LOG_FILE_NAMES[i] + "." + keyDate;
      S3Object object = s3Client.getObject(new GetObjectRequest(AWS_RAW_DATA_BUCKET, fileKey));

      S3ObjectInputStream objectContent = object.getObjectContent();

      BufferedReader in = null;
      in = new BufferedReader(new InputStreamReader(objectContent));
      String log;
      try {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while ((log = in.readLine()) != null) {
          try {
            Map<String, Object> logMap = mapper.readValue(log,
                new TypeReference<Map<String, Object>>() {});

            long timestamp = MapUtils.getLong(logMap, "timestamp");

            Map<String, Object> logCollectMap = new HashMap<String, Object>();
            logCollectMap.put("PAYLOAD", log);

            logCollectMap.put("@TIMESTAMP", timestamp);

            // 인덱스, 타입
            bulkRequest.add(client.prepareIndex("my_log", "log_type")
                .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("PAYLOAD", logMap)
                    .field("@TIMESTAMP", timestamp)
                    .endObject()
                )
            );
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
          System.out.println("ERROR");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    client.close();
  }

  private Client getElasticSearchClient() {
    Settings settings = ImmutableSettings
        .settingsBuilder().put("cluster.name", ES_CLUSTER_NAME).build();
    TransportClient transportClient = new TransportClient(settings);
    transportClient = transportClient
        .addTransportAddress(new InetSocketTransportAddress(ES_HOST, ES_PORT));

    return (Client) transportClient;
  }

  private void setAwsConfig() {
    AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
    s3Client = new AmazonS3Client(credentials);
    s3Client.setRegion(com.amazonaws.regions.Region.getRegion(Regions.AP_NORTHEAST_1));
  }

}
