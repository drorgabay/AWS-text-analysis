import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

public class ManagerTask implements Runnable {
    Ec2Client ec2;
    SqsClient sqs;
    S3Client s3;
    String managerWorkersSQS;
    String workersManagerSQS;
    String localApplicationID;
    int n;

    public ManagerTask(Ec2Client ec2, SqsClient sqs, S3Client s3, String managerWorkersSQS, String workersManagerSQS, String localApplicationID, int n) {
        this.ec2 = ec2;
        this.sqs = sqs;
        this.s3 = s3;
        this.managerWorkersSQS = managerWorkersSQS;
        this.workersManagerSQS = workersManagerSQS;
        this.localApplicationID = localApplicationID;
        this.n = n;

    }

    public void run() {
//        InputStream inputFile = s3.getObject(GetObjectRequest.builder()
//                .bucket("inputdt").key(localApplicationID).build());

//        FileReader fileReader = new FileReader()
        ResponseInputStream<GetObjectResponse> res = s3.getObject(GetObjectRequest.builder().bucket("inputdt").key(localApplicationID+".txt").build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(res));
        AWSAbstractions.addLocalApp(localApplicationID);
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("read line in ManagerTask\n");
                String[] message = line.split("\t");
                String typeOfAnalysis = message[0];
                String requiredFile = message[1];
                String taskID = UUID.randomUUID().toString();
                AWSAbstractions.incNumOfTasksOfLocalApp(localApplicationID);
                AWSAbstractions.addTaskIDToLocalApp(localApplicationID, taskID);
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(managerWorkersSQS)
                        .messageBody(localApplicationID + "\t" + typeOfAnalysis + "\t" + requiredFile + "\t" + taskID).build());
            }


            int activeWorkers = AWSAbstractions.getWorkersSize();
            int numOfTasks = AWSAbstractions.numOfTasks(localApplicationID);
            int workersForJob = (numOfTasks % n) == 0 ? (numOfTasks/n) : (numOfTasks/n)+1;
            int maxSizeOfWorkers = 18;
            if (maxSizeOfWorkers - activeWorkers < workersForJob)
                workersForJob = maxSizeOfWorkers - activeWorkers;
            int numOfWorkers = workersForJob;

            System.out.println("activeWorkers" + activeWorkers);
            System.out.println("numOfTasks" + numOfTasks);
            System.out.println("workersForJob" + workersForJob);
            System.out.println("numOfWorkers" + numOfWorkers);

                for (int i = 0; i < numOfWorkers; i++) {
                    String value = "#! /bin/bash\n" + "wget --no-check-certificate --no-proxy https://modeldt.s3.amazonaws.com/englishPCFG.ser.gz\n" + "wget --no-check-certificate --no-proxy https://jarsdt.s3.amazonaws.com/worker-1.0.jar\n" + "java -jar worker-1.0.jar\n";
                    String user_data = Base64.getEncoder()
                            .encodeToString(value.getBytes(StandardCharsets.UTF_8.toString()));
                    String workerInstanceId = AWSAbstractions
                            .createEC2Instance(ec2, "worker", user_data);
                    System.out.println("create worker num: " + i+1 + "\n");
                    AWSAbstractions.addWorker(workerInstanceId);
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
