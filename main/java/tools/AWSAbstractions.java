package tools;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AWSAbstractions {
    private static ConcurrentHashMap<String, List<String>> workers = new ConcurrentHashMap<>();
    private static AtomicInteger workersSize = new AtomicInteger();
    private static ConcurrentHashMap<String, Integer> localApplications = new ConcurrentHashMap<>(); // localAppId to num of tasks
    private static ConcurrentHashMap<String, List<String>> tasksID = new ConcurrentHashMap<>();     // localAppId to List of tasks

    public static void deleteMessageFromQueue(String url, Message msg, SqsClient sqs) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(url)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    private static String amiId = "ami-0cc8b7128543961d7";

    public static void reassignTasks(String id) {
        SqsClient sqs = getSQSClient();
        String ManagerWorkersSQS = AWSAbstractions.queueSetup(sqs, "manager-to-workers-queue");
        List<String> tasks = workers.get(id);
        for (String task: tasks){
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(ManagerWorkersSQS).messageBody(task).build());
        }
        workers.remove(id);
    }

    public static void addTaskIDToLocalApp(String localApplicationID, String taskID) {
        tasksID.get(localApplicationID).add(taskID);
    }

    public static List<String> getTasksOfLocalApp(String localAppID) {
        return tasksID.get(localAppID);
    }
    
    public static void addWorker(String id){
        workersSize.incrementAndGet();
        workers.put(id, new ArrayList<>());
    }

    public static void addTaskToWorker(String id, String task){
        workers.get(id).add(task);
    }

    public static void addLocalApp(String id){
        localApplications.put(id, 0);
        tasksID.put(id, new ArrayList<>());
    }
    public static void removeLocalApp(String id){
        localApplications.remove(id);
    }
    public static int numOfAllJobs(){
        return localApplications.size();
    }

    public static void doneTask(String localAppID, String workerId, String task){
                decNumOfTasksOfLocalApp(localAppID);
                removeTaskFromWorker(workerId, task);
    }

    public static int numOfTasks(String id) {
        return localApplications.get(id);
    }

    public static void removeWorker(String id) {
        workers.remove(id);
        workersSize.decrementAndGet();
    }

    public static void incNumOfTasksOfLocalApp(String localAppId){
        localApplications.put(localAppId, (localApplications.get(localAppId)+1));
    }
    public static void decNumOfTasksOfLocalApp(String localAppId){
        localApplications.put(localAppId, (localApplications.get(localAppId)-1));
    }
    public static void removeTaskFromWorker(String workerId, String task){
        workers.get(workerId).remove(task);
    }

    public static List<String> getWorkersList(){
        return new ArrayList<>(workers.keySet());
    }

    public static int getWorkersSize(){
        return workersSize.get();
    }

    public static String createEC2Instance(Ec2Client ec2, String name, String user_data) {
        IamInstanceProfileSpecification iamInstanceProfile = IamInstanceProfileSpecification.builder()
                .arn("arn:aws:iam::578663173380:instance-profile/LabInstanceProfile")
                .build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(iamInstanceProfile)
                .userData(user_data)
                .keyName("vockey")
                .securityGroupIds("launch-wizard-2")
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s\n",
                    instanceId, amiId);
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public static String queueSetup(SqsClient sqs, String queueName) {
        String queueUrl;
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (QueueNameExistsException e) {
//            throw e;
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        }
        System.out.println(queueName + ": Queue url initialized\n");
        return queueUrl;
    }

    public static SqsClient getSQSClient(){
        return SqsClient.builder().region(Region.US_EAST_1).build();
    }
    public static S3Client getS3Client(){
        return S3Client.builder().region(Region.US_EAST_1).build();
    }
    public static Ec2Client getEC2Client(){
        return Ec2Client.builder().region(Region.US_EAST_1).build();
    }
}
