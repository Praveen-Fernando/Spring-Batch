package com.example.springBatchChunk.service;

import com.example.springBatchChunk.model.StudentCsv;
import com.example.springBatchChunk.model.StudentResponse;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

public class StudentService {
    List<StudentResponse> list;

    public List<StudentResponse> restCallToGetStudents() {
        RestTemplate restTemplate = new RestTemplate();
        StudentResponse[] studentResponseArray =
                restTemplate.getForObject("http://localhost:8082/api/v1/students",
                        StudentResponse[].class);

        list = new ArrayList<>();

        for (StudentResponse sr : studentResponseArray) {
            list.add(sr);
        }

        return list;
    }

    public StudentResponse getStudent(long id, String name) {
        System.out.println("id = " + id + " and  name = " + name);
        if (list == null) {
            restCallToGetStudents();
        }

        if (list != null && !list.isEmpty()) {
            return list.remove(0);
        }
        return null;
    }

    public StudentResponse restCallToCreateStudent(StudentCsv studentCsv) {
        RestTemplate restTemplate = new RestTemplate();

        return restTemplate.postForObject("http://localhost:8082/api/v1/createStudent",
                studentCsv,
                StudentResponse.class);

    }

}
