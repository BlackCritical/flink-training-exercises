package com.example.bigdatareader;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BigDataReaderApplicationTests {

    @Test
    public void contextLoads() throws JsonProcessingException {
        for (int i = 0; i < 2; i++) {
            System.out.println(i);
        }
        for (int year = 2014; year < 2020; year++) {
            for (int month = 1; month < 12; month++) {
                final int fYear = year;
                final int fMonth = month;
                System.out.println(String.format("%04d-%02d-01", fYear, fMonth));
                System.out.println(String.format("%04d-%02d-01", fYear, fMonth + 1));
                System.out.println(" ");
            }
        }
//        ObjectMapper mapper = new ObjectMapper();
//        EarthquakeCollection e = new EarthquakeCollection("asfdsddsf", new Metadata(), new LinkedList<>(), new LinkedList<>());
//        System.out.println(mapper.writeValueAsString(e));
//
//        String url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-02-01&endtime=2014-03-01";
//        int startIndex = url.indexOf("starttime=");
//        int endIndex = url.indexOf("endtime=");
//        String startTime = url.substring(startIndex + 10, startIndex + 20);
//        String endTime = url.substring(endIndex + 8, endIndex + 18);
//        System.out.println(startTime);
//        System.out.println(endTime);
//
//
//        System.out.println(String.format("2014-%02d-01", 12));
//        System.out.println(String.format("2014-%02d-01", 5));
    }

}
