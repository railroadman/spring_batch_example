package spring_batch.batch.tasklet.job;

import jdk.jshell.spi.ExecutionControl;
import lombok.extern.apachecommons.CommonsLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import spring_batch.batch.mapper.AnimalFieldSetMapper;
import spring_batch.batch.model.Animal;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@StepScope
public class Reader implements Tasklet, StepExecutionListener {

    private String input;
    private FlatFileItemReaderBuilder<Animal> builder;
    private FieldSetMapper<Animal> animalFieldSetMapper;
    private static final String[] TOKENS = {"id", "name"};
    private ExecutionContext executionContext;
    private JobParameters jobParameters;
    List<Animal> animals = new ArrayList<>();

    @Override
    public void beforeStep(StepExecution stepExecution) {
        builder = new FlatFileItemReaderBuilder<>();
        animalFieldSetMapper = new AnimalFieldSetMapper();
        executionContext = stepExecution.getJobExecution().getExecutionContext();
        jobParameters = stepExecution.getJobParameters();
    }
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext context) throws Exception {
        input = jobParameters.getString("file.input");
        FlatFileItemReader<Animal> fileReader = builder
                .name("animalReader")
                .resource(new FileSystemResource(input))
                .delimited()
                .names(TOKENS)
                .fieldSetMapper(animalFieldSetMapper)
                .build();
        fileReader.open(executionContext);

        do {
            Animal animal = fileReader.read();
            animals.add(animal);
        } while (fileReader.read() != null);
        animals.remove(animals.size());
        return RepeatStatus.FINISHED;
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        executionContext.put("animals", animals);
        log.debug("Lines Reader ended.");
        return ExitStatus.COMPLETED;
    }
}
