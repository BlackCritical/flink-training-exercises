package com.example.bigdatareader.controller;

import com.example.bigdatareader.model.EarthquakeCollection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("Duplicates")
@Controller
public class RESTController {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RESTController.class);
    private static EarthquakeCollection res;

    private static final WebClient webClient = WebClient.builder()
            .baseUrl("https://earthquake.usgs.gov/fdsnws/event/1/")
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .build();

    private static void saveResultToFile() {
        log.info("Write to file!");
        try (PrintWriter out = new PrintWriter("earthquake.json"); PrintWriter out2 = new PrintWriter("features.json")) {
            out.println(mapper.writeValueAsString(res));
            out2.println(mapper.writeValueAsString(res.features));
        } catch (FileNotFoundException | JsonProcessingException e) {
            log.info(res.toString());
            e.printStackTrace();
        }
    }

    @GetMapping(value = "/start")
    public String getEarthquakesNonBlocking() {
        log.info("Starting NON-BLOCKING Controller!");
        List<Flux<EarthquakeCollection>> requests = new LinkedList<>();

        for (int year = 2014; year < 2020; year++) {
            for (int month = 1; month < 12; month++) {
                log.info(year + " " + month);
                final int fYear = year;
                final int fMonth = month;
                Flux<EarthquakeCollection> earthquakeFlux1 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-01", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-05", fYear, fMonth))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);
                Flux<EarthquakeCollection> earthquakeFlux2 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-05", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-10", fYear, fMonth))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);
                Flux<EarthquakeCollection> earthquakeFlux3 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-10", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-15", fYear, fMonth))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);
                Flux<EarthquakeCollection> earthquakeFlux4 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-15", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-20", fYear, fMonth))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);
                Flux<EarthquakeCollection> earthquakeFlux5 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-20", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-25", fYear, fMonth))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);

                Flux<EarthquakeCollection> earthquakeFlux6 = webClient
                        .get()
                        .uri(builder -> builder.path("/query")
                                .queryParam("format", "geojson")
                                .queryParam("starttime", String.format("%04d-%02d-25", fYear, fMonth))
                                .queryParam("endtime", String.format("%04d-%02d-01", fYear, fMonth + 1))
                                .build())
                        .retrieve()
                        .bodyToFlux(EarthquakeCollection.class);

                requests.add(earthquakeFlux1);
                requests.add(earthquakeFlux2);
                requests.add(earthquakeFlux3);
                requests.add(earthquakeFlux4);
                requests.add(earthquakeFlux5);
                requests.add(earthquakeFlux6);
            }
        }

        Flux.concat(requests).subscribe(
                earthquakeCollection -> {
                    log.info(earthquakeCollection.type);

                    if (res == null) {
                        res = earthquakeCollection;
                        res.allMetadata.add(res.metadata);
                        res.metadata = null;
                    } else if (earthquakeCollection.metadata.count > 0 && !earthquakeCollection.bbox.isEmpty()) {
                        res.features.addAll(earthquakeCollection.features);
                        res.allMetadata.add(earthquakeCollection.metadata);

                        List<Double> doubles = earthquakeCollection.bbox;
                        List<Double> resultBbox = res.bbox;
                        for (int i = 0; i < 3; i++) {
                            Double bbox = doubles.get(i);
                            resultBbox.set(i, resultBbox.get(i) < bbox ? resultBbox.get(i) : bbox);
                        }
                        for (int i = 3; i < 6; i++) {
                            Double bbox = doubles.get(i);
                            resultBbox.set(i, resultBbox.get(i) > bbox ? resultBbox.get(i) : bbox);
                        }
                    }
                },
                throwable -> {
                    throwable.printStackTrace();
                    saveResultToFile();
                },
                RESTController::saveResultToFile);
        log.info("Exiting NON-BLOCKING Controller!");
        return "success.html";
    }
}
