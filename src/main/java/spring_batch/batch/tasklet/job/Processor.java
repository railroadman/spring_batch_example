package spring_batch.batch.tasklet.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import spring_batch.batch.model.Animal;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
@StepScope
public class Processor implements Tasklet, StepExecutionListener {

    private List<Animal> animals;
    private ExecutionContext executionContext;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        animals = (List<Animal>)stepExecution.getJobExecution().getExecutionContext().get("animals");
        if (animals != null) animals.forEach(a -> a.setName(a.getName().toUpperCase()));
        log.debug("Lines Processor initialized.");

    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return ExitStatus.COMPLETED;
    }
}
