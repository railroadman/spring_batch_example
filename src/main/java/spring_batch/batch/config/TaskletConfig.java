package spring_batch.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import spring_batch.batch.tasklet.job.Processor;
import spring_batch.batch.tasklet.job.Reader;
import spring_batch.batch.tasklet.job.Writer;

@Configuration
@EnableBatchProcessing
public class TaskletConfig {
    @Autowired
    private JobBuilderFactory jobBuilder;

    @Autowired
    private StepBuilderFactory stepBuilder;

    @Bean
    protected Step processAnimals() {
        return stepBuilder
                .get("processAnimals")
                .tasklet(new Processor())
                .build();
    }

    @Bean
    protected Step writeAnimals() {
        return stepBuilder
                .get("writeAnimals")
                .tasklet(new Writer())
                .build();
    }

    @Bean
    protected Step readAnimals() {
        return stepBuilder
                .get("readAnimals")
                .tasklet(new Reader())
                .build();
    }

    @Bean
    public Job taskletJob() {
        return jobBuilder
                .get("taskletsJob")
                .start(readAnimals())
                .next(processAnimals())
                .next(writeAnimals())
                .build();
    }




}
