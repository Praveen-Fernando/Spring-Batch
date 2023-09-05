package com.example.springBatchChunk.processor;

import com.example.springBatchChunk.model.StudentJdbc;
import com.example.springBatchChunk.model.StudentJson;
import org.springframework.stereotype.Component;
import org.springframework.batch.item.ItemProcessor;

@Component
public class FirstItemProcessor implements ItemProcessor<StudentJdbc, StudentJson> {

    @Override
    public StudentJson process(StudentJdbc item) {

        System.out.println("____Inside the Item Processor____");
        StudentJson studentJson = new StudentJson();
        studentJson.setId(item.getId());
        studentJson.setFirstName(item.getFirstName());
        studentJson.setLastName(item.getLastName());
        studentJson.setEmail(item.getEmail());
        return studentJson;
    }
}
