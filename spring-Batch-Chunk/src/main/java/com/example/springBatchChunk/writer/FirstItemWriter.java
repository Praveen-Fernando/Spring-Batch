package com.example.springBatchChunk.writer;

import com.example.springBatchChunk.model.StudentResponse;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class FirstItemWriter implements ItemWriter<StudentResponse>{

    public void write(List<? extends StudentResponse> items) throws Exception {
        System.out.println("____Inside the Item Writer____");
        items.stream().forEach(System.out::println);
    }
}