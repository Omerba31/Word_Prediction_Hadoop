import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static int numberOfInstances = 7;

    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");

        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Config.REGION)
                .build();

        HadoopJarStepConfig Step1_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step1.jar")
                .withMainClass("Step1");

        StepConfig Step1_Config = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(Step1_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig Step2_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step2.jar")
                .withMainClass("Step2");

        StepConfig Step2_Config = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(Step2_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig Step3_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step3.jar")
                .withMainClass("Step3");

        StepConfig Step3_Config = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(Step3_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig Step4_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step4.jar")
                .withMainClass("Step4");

        StepConfig Step4_Config = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(Step4_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("3.4.1")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(Step1_Config, Step2_Config, Step3_Config, Step4_Config)
                .withLogUri(Config.LOGS)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
