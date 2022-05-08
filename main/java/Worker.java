import javafx.scene.Parent;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import tools.AWSAbstractions;
import tools.Parser;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

//        ▪ Gets a message from an SQS queue. V
//        ▪ Downloads the text file indicated in the message. V
//        ▪ Performs the requested analysis on the file.
//        ▪ Uploads the resulting analysis file to S3.
//        ▪ Puts a message in an SQS queue indicating the original URL of the input file, the S3 url of the
//          analyzed file, and the type of the performed analysis.
//        ▪ Removes the processed message from the SQS queue.
//        ▪ If an exception occurs, the worker should recover from it, send a message to the manager of
//        the input message that caused the exception together with a short description of the
//        exception, and continue working on the next message.
//        ▪ If a worker stops working unexpectedly before finishing its work on a message, some other
//        worker should be able to handle that message.

public class Worker {
    private static String managerWorkersSQS;
    private static String workersManagerSQS;
    private static SqsClient sqs;
    private static S3Client s3;
    private static List<Message> messageQueue;

    private static String parserLocation = "";


    public static void deleteMessage(String url, Message msg, SqsClient sqs) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(url)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public static void main(String[] args) {
        sqs = AWSAbstractions.getSQSClient();
        s3 = AWSAbstractions.getS3Client();
        managerWorkersSQS = AWSAbstractions.queueSetup(sqs, "manager-to-workers-queue");
        workersManagerSQS = AWSAbstractions.queueSetup(sqs, "workers-to-manager-queue");
        for (; ; ) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(managerWorkersSQS)
                .waitTimeSeconds(20)    // wait 20 sec before saying there were no message
                .maxNumberOfMessages(1)
                .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                // worker ID
                String workerID = EC2MetadataUtils.getInstanceId();
                String taskID = message[3];
                System.out.println("worker id: "+ workerID+ "message: "+ messages.get(0).body());
                // acknowledge message for manager
                String ackMsg = "ack task" + "\t" + workerID + "\t" + messages.get(0).body();
                // tell manager what task we took
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(workersManagerSQS).messageBody(ackMsg).build());
                deleteMessage(managerWorkersSQS, messages.get(0), sqs);

                //String taskKey = UUID.randomUUID().toString(); // for uploading the file
                // message[0] = local application ID, message[1] = type of analysis type, message[2] = url of input file
                try {
                    System.out.println("worker start to parsing\n");
                    String localAppId = message[0], type = message[1], url = message[2];
                     File parsingResult = Parser.parseFile(url, type);
                     System.out.println("done parsing");
                    PutObjectResponse file = s3.putObject(PutObjectRequest.builder().bucket("summarydt").key(localAppId + "/" + taskID).build(), RequestBody.fromFile(parsingResult));
                    // send message to the manager
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(workersManagerSQS).messageBody("done task" + "\t" + localAppId + "\t" + file+ "\t"+ type + "\t" + url + "\t" + workerID + "\t" + taskID).build());

                } catch (Exception e) {
                    String url = message[2];
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(workersManagerSQS).messageBody(Arrays.toString(e.getStackTrace()) + "\t" + url).build());
                }
                }
        }
    }
}