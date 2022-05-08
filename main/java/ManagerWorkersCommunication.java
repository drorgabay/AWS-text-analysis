import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;
import java.io.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ManagerWorkersCommunication implements Runnable {


    @Override
    public void run() {
        Ec2Client ec2 = AWSAbstractions.getEC2Client();
        SqsClient sqs = AWSAbstractions.getSQSClient();
        S3Client s3 = AWSAbstractions.getS3Client();
        String workersManagerSQS = AWSAbstractions.queueSetup(sqs, "workers-to-manager-queue");
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(workersManagerSQS)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();
            List<Message> messages;
            messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                if (message[0].equals("ack task")) { // worker announces it took a message
                    // worker protocol "ack task" + "\t" + workerID + "\t" + messages.get(0).body();
                    String workerID = message[1];
                    String task = message[2];
                    AWSAbstractions.addTaskToWorker(workerID, task);


                } else if (message[0].equals("done task")) {   //worker announces it finished a task
                    String localApplicationID = message[1];
                    String managerLocalSQS = AWSAbstractions.queueSetup(sqs, "manager-to-local-queue-" + localApplicationID);
                    sqs.sendMessage((SendMessageRequest.builder().queueUrl(managerLocalSQS).messageBody("test1" + "\t" + localApplicationID).build()));
                    // worker protocol "done task" + "\t" + localAppId + "\t" + file+ "\t"+ type + "\t" + url + "\t" + workerID
                    String requiredFile = message[4], typeOfAnalysis = message[3], workerID = message[5];
                    String task = localApplicationID + "\t" + typeOfAnalysis + "\t" + requiredFile;
                    AWSAbstractions.doneTask(localApplicationID, workerID, task); // lock?
                    if (AWSAbstractions.numOfTasks(localApplicationID) == 0) {
                        AWSAbstractions.removeLocalApp(localApplicationID);
                        sqs.sendMessage((SendMessageRequest.builder().queueUrl(managerLocalSQS).messageBody("test2" + "\t" + localApplicationID).build()));
                        if (Thread.currentThread().isInterrupted() && AWSAbstractions.numOfAllJobs() == 0) {
                            for (String worker : AWSAbstractions.getWorkersList()) {
                                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(worker).build());
                                AWSAbstractions.removeWorker(worker);
                                System.out.println("terminating worker: " + worker);
                            }
                            break;
                        }

                        try
                        {
                            String result = "";
                            System.out.println("Tasks: " + AWSAbstractions.getTasksOfLocalApp(localApplicationID).size());
                            for (String taskID : AWSAbstractions.getTasksOfLocalApp(localApplicationID)) {
                                System.out.println("inside loop\n");
                                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                InputStream inputStream = s3.getObject(GetObjectRequest.builder().bucket("summarydt").key(localApplicationID + "/" + taskID).build());
                                for (int data = inputStream.read(); data != -1; data = inputStream.read()) {
                                    byteArrayOutputStream.write(data);
                                }
                                result = result.concat(byteArrayOutputStream.toString("UTF-8"));
                            }
                            s3.putObject(PutObjectRequest.builder().bucket("outputdt").key(localApplicationID).build(), RequestBody.fromString(result));
                            sqs.sendMessage((SendMessageRequest.builder().queueUrl(managerLocalSQS).messageBody("test4" + "\t" + localApplicationID).build()));
                            sqs.sendMessage((SendMessageRequest.builder().queueUrl(managerLocalSQS).messageBody("done job\t" + localApplicationID).build()));
                        }
                        catch (Exception e){

                        }
                    }
                } else {// worker exception – (e + url)
                    String e = message[0];
                    String url = message[1];
                    System.out.println("There was problem at a task at url – " + url +"\nException: " + e);
                }
                AWSAbstractions.deleteMessageFromQueue(workersManagerSQS, messages.get(0), sqs);
            }
        }
    }
}
