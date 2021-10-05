package spring_batch.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class BatchApplication implements CommandLineRunner {
	@Autowired
	private Job job;
	@Autowired
	private Job taskletJob;
	@Autowired
	private JobLauncher jobLauncher;
	@Value("${file.input}")
	private String input;

	@Value("${file.output}")
	private String output;
	@Value("${file.output2}")
	private String output2;



	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);

	}

	@Override
	public void run(String... args) throws Exception {
		JobParametersBuilder jobParameters = new JobParametersBuilder();
		jobParameters.addString("file.input", input);
		jobParameters.addString("file.output", output);
		jobParameters.addString("file.output2", output2);
		jobParameters.addString("trial","52");

		try {
			launchJob(job,jobParameters.toJobParameters());
			launchJob(taskletJob,jobParameters.toJobParameters());
		}
		catch (JobExecutionAlreadyRunningException ex){
			log.info("job:{} already running\n Details {}",job.getName(),ex.getStackTrace());
		}
		catch (JobInstanceAlreadyCompleteException ex){
			log.info("job:{} already completed\n PARAMS:{}\n Details {}",job.getName(),
																		 jobParameters.toJobParameters().toString(),
																	     ex.getStackTrace());

		}
		catch (JobParametersInvalidException ex){
			log.error("job:{} INVALID PARAMS\n PARAMS:{}\n Details {}",job.getName(),
					jobParameters.toJobParameters().toString(),
					ex.getStackTrace());
		}
		catch (JobRestartException  ex) {
			log.error("job:{} RESTART TROUBLE \n PARAMS:{}\n Details {}",job.getName(),
					jobParameters.toJobParameters().toString(),
					ex.getStackTrace());
		}

	}

	private void launchJob (Job job , JobParameters params) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
		jobLauncher.run(job, params);

	}



}
