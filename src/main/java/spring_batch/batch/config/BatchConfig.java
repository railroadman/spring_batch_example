package spring_batch.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import spring_batch.batch.mapper.AnimalFieldSetMapper;
import spring_batch.batch.model.Animal;

import java.io.IOException;


@EnableBatchProcessing
@Configuration
public class BatchConfig {
    private  static final Logger LOGGER =  LoggerFactory.getLogger(BatchConfig.class);
    @Autowired
    private JobBuilderFactory jobBuilder;
    @Autowired
    private StepBuilderFactory stepBuilder;
    private static final String[] TOKENS = {"id", "name"};



    @Bean
    public Job job(Step step) {
            return jobBuilder.get("job")
                    .flow(step)
                    .end()
                    .build();
            }
    @Bean
    public Step step(ItemReader<Animal> csvItemReader, ItemWriter<Animal> jsonItemWriter) throws IOException {
        return stepBuilder
                .get("step1")
                .<Animal, Animal>chunk(10000)
                .reader(csvItemReader)
                .processor(animalItemProcessor())
                .writer(jsonItemWriter)
                .build();

    }
    @Bean
    @StepScope
    public FlatFileItemReader<Animal> csvItemReader(@Value("#{jobParameters['file.input']}") String input) {
        FlatFileItemReaderBuilder<Animal> builder = new FlatFileItemReaderBuilder<>();
        FieldSetMapper<Animal> animalFieldSetMapper = new AnimalFieldSetMapper();

        return builder
                .name("animalReader")
                .resource(new FileSystemResource(input))
                .delimited()
                .names(TOKENS)
                .fieldSetMapper(animalFieldSetMapper)
                .build();
    }
    @Bean
    @StepScope
    ItemProcessor<Animal, Animal> animalItemProcessor() {
        return animal -> {
            LOGGER.info("Processing " + animal.getName());
            //animal.setName(animal.getName().toUpperCase());
            return animal;
        };
    }

    @Bean
    @StepScope
    public JsonFileItemWriter<Animal> jsonItemWriter(@Value("#{jobParameters['file.output']}") String output) throws IOException {
        JsonFileItemWriterBuilder<Animal> builder = new JsonFileItemWriterBuilder<>();
        JacksonJsonObjectMarshaller<Animal> marshaller = new JacksonJsonObjectMarshaller<>();
        return builder
                .name("animalWriter")
                .jsonObjectMarshaller(marshaller)
                .resource(new FileSystemResource(output))
                .build();
    }


}