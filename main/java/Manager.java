import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import tools.AWSAbstractions;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Manager {

    private static String localManagerSQS;
    private static String managerWorkersSQS;
    private static String workersManagerSQS;
    private static String managerLocalSQS;
    // NEED TO ADD SYNC
    private static int numOfThreads = 15;

    public static void main(String args[]) {
        System.out.println("manager started:\n");
        Ec2Client ec2 = Ec2Client.create(); // connect to EC2 service
        S3Client s3 = S3Client.create(); // connect to S3 service
        SqsClient sqs = SqsClient.create(); // connect to SQS service
        localManagerSQS = AWSAbstractions.queueSetup(sqs, "local-to-manager-queue");
        managerWorkersSQS = AWSAbstractions.queueSetup(sqs, "manager-to-workers-queue");
        workersManagerSQS = AWSAbstractions.queueSetup(sqs, "workers-to-manager-queue");
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
        boolean terminate = false;

        String managerID = null;
        DescribeInstancesRequest req = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse res = ec2.describeInstances(req);
        for (Reservation reservation : res.reservations()) {
            for (Instance instance : reservation.instances()) {
                String name = instance.tags().get(0).value();
                String state = instance.state().name().toString();
                if (name.equals("manager") && (state.equals("running") || state.equals("pending")))
                    managerID = instance.instanceId();
            }
        }

        Runnable workerConnection = new WorkerConnection();
        Runnable managerWorkerCommunication = new ManagerWorkersCommunication();
        executor.execute(workerConnection);
        executor.execute(managerWorkerCommunication);
        while (true) {
            System.out.println("while not terminate, manager still wait for a message from local apps\n");
            // a request to receive only one message
            ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(localManagerSQS)
                    .maxNumberOfMessages(1).waitTimeSeconds(20).build();
            List<Message> messages;
            messages = sqs.receiveMessage(request).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                AWSAbstractions.deleteMessageFromQueue(localManagerSQS, messages.get(0), sqs);
                if (message[0].equals("terminate")) {
                    System.out.println("manager needs to terminate!\n");
                    //executor.shutdownNow();
                    while (true) {
                        if (AWSAbstractions.getWorkersSize() == 0) {
                            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(managerID).build());
                            break;
                        }
                        try {
                            TimeUnit.SECONDS.sleep(180);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                } else if (message[0].equals("new job")) {
                    System.out.println("manager got a new job message\n");
                    int n = Integer.parseInt(message[1]);
                    String localApplicationID = message[2];
                    Runnable job = new ManagerTask(ec2, sqs, s3, managerWorkersSQS, workersManagerSQS, localApplicationID, n);
                    executor.execute(job);
                }
            }
        }
    }
}