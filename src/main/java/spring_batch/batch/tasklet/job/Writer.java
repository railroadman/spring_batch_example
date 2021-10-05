package spring_batch.batch.tasklet.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import spring_batch.batch.model.Animal;

import java.util.List;

@Slf4j
@Component
@StepScope
public class Writer implements Tasklet, StepExecutionListener {
    private List<Animal> animals;
    JsonFileItemWriter<Animal> animalWriter;
    private JobParameters jobParameters;
    private ExecutionContext executionContext;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        jobParameters = stepExecution.getJobParameters();
        executionContext = stepExecution.getExecutionContext();
        animals = (List<Animal>) stepExecution.getJobExecution().getExecutionContext().get("animals");
        JsonFileItemWriterBuilder<Animal> builder = new JsonFileItemWriterBuilder<>();
        JacksonJsonObjectMarshaller<Animal> marshaller = new JacksonJsonObjectMarshaller<Animal>();
        animalWriter = builder
                .name("animalWriter")
                .jsonObjectMarshaller(marshaller)
                .resource(new FileSystemResource(jobParameters.getString("file.output2")))
                .build();
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
            animalWriter.open(executionContext);
            animalWriter.write(animals);
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        animalWriter.close();
        return ExitStatus.COMPLETED;
    }

}
