package com.example.springBatchChunk.config;

import com.example.springBatchChunk.model.StudentCsv;
import com.example.springBatchChunk.model.StudentJdbc;
import com.example.springBatchChunk.model.StudentJson;
import com.example.springBatchChunk.model.StudentXml;
import com.example.springBatchChunk.processor.FirstItemProcessor;
import com.example.springBatchChunk.reader.FirstItemReader;
import com.example.springBatchChunk.service.StudentService;
import com.example.springBatchChunk.writer.FirstItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

@Configuration
public class SampleJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private FirstItemReader firstItemReader;
    @Autowired
    private FirstItemProcessor firstItemProcessor;
    @Autowired
    private FirstItemWriter firstItemWriter;
    @Autowired
    private StudentService studentService;

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource datasource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.universitydatasource")
    public DataSource universitydatasource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public Job chunkJob() {
        return jobBuilderFactory.get("Chunk Job")
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep())
                .build();
    }


    private Step firstChunkStep() {
        return stepBuilderFactory.get("First Chunk Step")
                //CSV
                .<StudentCsv, StudentCsv>chunk(5)
                .reader(flatFileItemReader(null))

                //JSON
//                .<StudentJson, StudentJson>chunk(5)
//                .reader(jsonItemReader(null))

                //XML
//                .<StudentXml, StudentXml>chunk(5)
//                .reader(studentXmlStaxEventItemReader(null))

                //JDBC
//                .<StudentJdbc, StudentJdbc>chunk(5)
//                .reader(jdbcJdbcCursorItemReader())

                //Rest Service
//                .<StudentResponse, StudentResponse>chunk(5)
//                .reader(itemReaderAdapter())

//                .processor(firstItemProcessor)
//                .writer(firstItemWriter)

                //CSV Writer
//                .writer(flatFileItemWriter(null))

                //JSON Writer
//                .writer(jsonFileItemWriter(null))

                //XML Writer
//                .writer(staxEventItemWriter(null))

                //JDBC
//                .writer(jdbcBatchItemWriter())

                //Prepared Statement
//                .writer(jdbcBatchItemWriter1())

                //Rest API
                .writer(itemWriterAdapter())
                .build();
    }

    //Item Reader
    //Flat file - CSV
    @Bean
    @StepScope
    public FlatFileItemReader<StudentCsv> flatFileItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

        //Source
        //flatFileItemReader.setResource(new FileSystemResource(new File("InputFiles\\students.csv")));
        flatFileItemReader.setResource(fileSystemResource);

        //LineMapper
        flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("ID", "First Name", "Last Name", "Email");
                    }
                });

                setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>() {
                    {
                        setTargetType(StudentCsv.class);
                    }
                });
            }
        });

//        DefaultLineMapper<StudentCsv> defaultLineMapper = new DefaultLineMapper<StudentCsv>();
//
//        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
//        delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");
//
//        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
//
//        BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper = new BeanWrapperFieldSetMapper<StudentCsv>();
//        fieldSetMapper.setTargetType(StudentCsv.class);
//
//        defaultLineMapper.setFieldSetMapper(fieldSetMapper);
//
//        flatFileItemReader.setLineMapper(defaultLineMapper);


        //Skip 1st line
        flatFileItemReader.setLinesToSkip(1);


        return flatFileItemReader;

    }


    //JSON
    @Bean
    @StepScope
    public JsonItemReader<StudentJson> jsonItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        JsonItemReader<StudentJson> jsonItemReader = new JsonItemReader<StudentJson>();

        jsonItemReader.setResource(fileSystemResource);

        jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));

        //Read only 8 data items
        jsonItemReader.setMaxItemCount(8);

        //Skipping first 2 data items
        jsonItemReader.setCurrentItemCount(2);

        return jsonItemReader;
    }

    //XML
    @Bean
    @StepScope
    public StaxEventItemReader<StudentXml> studentXmlStaxEventItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        StaxEventItemReader<StudentXml> studentXmlStaxEventItemReader = new StaxEventItemReader<StudentXml>();

        studentXmlStaxEventItemReader.setResource(fileSystemResource);

        studentXmlStaxEventItemReader.setFragmentRootElementName("student");

        //Java object into XML - Marshalling
        //XMl into Java object - Unmarshalling(Use this for example)
        studentXmlStaxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
            {
                setClassesToBeBound(StudentXml.class);
            }
        });

        return studentXmlStaxEventItemReader;
    }

    //JDBC
    @Bean
    @StepScope
    public JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader() {
        JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader = new JdbcCursorItemReader<StudentJdbc>();

        jdbcJdbcCursorItemReader.setDataSource(universitydatasource());
        jdbcJdbcCursorItemReader.setSql("Select id, first_name as firstName, last_name as last_name, email from student");

        jdbcJdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<StudentJdbc>() {
            {
                setMappedClass(StudentJdbc.class);
            }
        });

        return jdbcJdbcCursorItemReader;
    }

    //    Rest Service
//    public ItemReaderAdapter<StudentResponse> itemReaderAdapter(){
//
//        ItemReaderAdapter<StudentResponse> itemReaderAdapter  = new ItemReaderAdapter<StudentResponse>();
//
//        itemReaderAdapter.setTargetObject(studentService);
//        itemReaderAdapter.setTargetMethod("getStudent");
//        itemReaderAdapter.setArguments(new Object[] {1L, "Test"});
//
//
//        return itemReaderAdapter;
//    }


    //______Item Writers______________________________________________________________________________________________________________________

    //Flat file - JDBC to Flat file
    @Bean
    @StepScope
    public FlatFileItemWriter<StudentJdbc> flatFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {

        FlatFileItemWriter<StudentJdbc> flatFileItemWriter = new FlatFileItemWriter<StudentJdbc>();

        flatFileItemWriter.setResource(fileSystemResource);

        //write the header names to CSV file
        flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("Id, First Name, Last Name, Email");
            }
        });

        //Extract data from JDBC
        flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJdbc>() {
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<StudentJdbc>() {
                    {
                        setNames(new String[]{"id", "firstName", "lastName", "email"});
                    }
                });
            }
        });

        // Write additional text to csv file
        flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("Created @ " + new Date());
            }
        });

        return flatFileItemWriter;
    }

    //JDBC to JSON
    @Bean
    @StepScope
    public JsonFileItemWriter<StudentJson> jsonFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {

        JsonFileItemWriter<StudentJson> jsonFileItemWriter = new JsonFileItemWriter<StudentJson>(
                fileSystemResource, new JacksonJsonObjectMarshaller<>());

        return jsonFileItemWriter;
    }

    //JDBC to XML
    @Bean
    @StepScope
    public StaxEventItemWriter<StudentJdbc> staxEventItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {

        StaxEventItemWriter<StudentJdbc> staxEventItemWriter = new StaxEventItemWriter<>();

        staxEventItemWriter.setResource(fileSystemResource);
        staxEventItemWriter.setRootTagName("students");
        staxEventItemWriter.setMarshaller(new Jaxb2Marshaller() {
            {
                setClassesToBeBound(StudentJdbc.class);
            }
        });
        return staxEventItemWriter;
    }


    //CSV to JDBC
    @Bean
    public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter() {

        JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();

        jdbcBatchItemWriter.setDataSource(universitydatasource());

        jdbcBatchItemWriter.setSql("insert into student(id, first_name, last_name, email) " +
                "values(:id , :firstName, :lastName, :email)");

        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<StudentCsv>());

        return jdbcBatchItemWriter;
    }

    //CSV to Prepared Statement(mysql)
    @Bean
    public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter1() {

        JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();

        jdbcBatchItemWriter.setDataSource(universitydatasource());

        jdbcBatchItemWriter.setSql("insert into student(id, first_name, last_name, email) " +
                "values(?,?,?,?");

        jdbcBatchItemWriter.setItemPreparedStatementSetter(
                new ItemPreparedStatementSetter<StudentCsv>() {
                    @Override
                    public void setValues(StudentCsv studentCsv, PreparedStatement preparedStatement) throws SQLException {
                        preparedStatement.setLong(1, studentCsv.getId());
                        preparedStatement.setString(2, studentCsv.getFirstName());
                        preparedStatement.setString(3, studentCsv.getLastName());
                        preparedStatement.setString(4, studentCsv.getEmail());
                    }
                });
        return jdbcBatchItemWriter;
    }


    //Rest Service
    public ItemWriterAdapter<StudentCsv> itemWriterAdapter() {

        ItemWriterAdapter<StudentCsv> itemWriterAdapter = new ItemWriterAdapter<StudentCsv>();

        itemWriterAdapter.setTargetObject(studentService);
        itemWriterAdapter.setTargetMethod("restCallToCreateStudent");

        return itemWriterAdapter;
    }


}