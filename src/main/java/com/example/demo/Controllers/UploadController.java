package com.example.demo.Controllers;

import com.example.demo.Services.UploadedFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

@RestController
public class UploadController {

    @Autowired
    UploadedFileService Service;
    @PostMapping("/Upload")
    public ResponseEntity<String> upload (@RequestParam("file") MultipartFile file , @RequestParam(name = "value") String value, @RequestParam(name="column") String column) throws IOException {

        return Service.upload(file,value,column);
    }

}
