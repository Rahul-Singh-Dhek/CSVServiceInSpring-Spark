package com.example.demo.Services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.ByteArrayOutputStream;
import java.nio.file.*;
import java.time.LocalDateTime;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
//import spark.Spark;
import javax.servlet.http.HttpServletResponse;
import java.util.regex.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.Files.lines;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

@Service
public class UploadedFileService {

    private static SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();

    public ResponseEntity<String> upload (MultipartFile file, String value, String column) throws IOException {
        LocalDateTime curTime=LocalDateTime.now();
        String curTime1=curTime.toString().replace(":","");
        Path path = Paths.get("C:\\Spring Service of Platform\\demo\\src\\main\\resources\\Download-File\\" +curTime1+file.getOriginalFilename());
        byte[] bytes=file.getBytes();
        Files.write(path,bytes);
        String Path=path.toString();
        Dataset<Row> csv = spark.read().option("header", "true").format("csv").csv(Path);
//        csv.show();

        Path="C:\\Spring Service of Platform\\demo\\src\\main\\resources\\Converted\\Converted-File"+curTime1;

//Removeing Exact Value---------------------------------------------------------------------------------------------------------
        csv.withColumn(column,when(col(column).equalTo(value),null).otherwise(col(column))).write().option("header",true)
                .csv(Path);
        File directoryPath = new File(Path);
        String contents[] = directoryPath.list();
        // Regex to check valid image file extension.
        String regex
                = "([^\\s]+(\\.(?i)(csv))$)";

        // Compile the ReGex
        Pattern p = Pattern.compile(regex);
        int partCount=0;
        for(int i=0; i<contents.length; i++) {
            System.out.println(contents[i]);
            Matcher m = p.matcher(contents[i]);
//            System.out.println(m.matches());
            String Path1=Path+"//"+contents[i];
            File f= new File(Path1);
            if(!m.matches()){
                System.out.println(f.delete());
            }else{
                ++partCount;
                Path path2 = Paths.get(Path1);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] arr = Files.readAllBytes(path2);
                out.write(arr);
                return ResponseEntity.ok()
                        .header("File-Name", contents[i])
                        .header("Content-Type","text/csv")
                        .body(out.toString());
            }
        }
return null;

    }

}
