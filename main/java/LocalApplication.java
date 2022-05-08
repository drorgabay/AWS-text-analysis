import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class LocalApplication {
    private static String inputFile;
    private static File outputFile;
    private static int n;
    private static String localApplocationId;
    private static S3Client s3;
    private static String localManagerSQS;
    private static String managerLocalSQS;
    private static Ec2Client ec2;
    private static SqsClient sqs;
    private static boolean terminate = false;

        private static List<Bucket> initializedBuckets(S3Client s3) {
        List<Bucket> buckets = s3.listBuckets().buckets();
        if (buckets.isEmpty()) {
            s3.createBucket(CreateBucketRequest.builder().bucket("inputdt").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("outputdt").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("summarydt").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("modeldt").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("jarsdt").build());
        }
//            s3.putObject(PutObjectRequest.builder().bucket("modeldt").key("englishPCFG.ser.gz").build(), RequestBody.fromFile(new File("src/main/resources/englishPCFG.ser.gz")));
            //s3.putObject(PutObjectRequest.builder().bucket("jarsdt").key("worker-1.0.jar").build(), RequestBody.fromFile(new File("src/main/resources/worker-1.0.jar")));
//            s3.putObject(PutObjectRequest.builder().bucket("jarsdt").key("manager-1.0.jar").build(), RequestBody.fromFile(new File("src/main/resources/manager-1.0.jar")));
//            s3.putObject(PutObjectRequest.builder().bucket("jarsdt").key("parser").build(), RequestBody.fromFile(new File("src/main/java/Parser")));
//            return s3.listBuckets().buckets();

        return buckets;
    }

    private static boolean checkActiveManager() {
        try {
            DescribeInstancesRequest req = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse res = ec2.describeInstances(req);
            for (Reservation reservation : res.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.tags().isEmpty())
                        continue;
                    String name = instance.tags().get(0).value();
                    String state = instance.state().name().toString();
                    if (name.equals("manager") && (state.equals("running") || state.equals("pending")))
                        return true;
                }
            }
            return false;
        } catch (Ec2Exception e) {
            System.out.println("problem in function: CheckActiveManager");
            System.out.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }

    private static void activateManager(String localApplocationId) {
        // create the queue between all locals to manager
        localManagerSQS = AWSAbstractions.queueSetup(sqs, "local-to-manager-queue");

        // create the queue between manager to all locals
        managerLocalSQS = AWSAbstractions.queueSetup(sqs, "manager-to-local-queue-"+localApplocationId);

        boolean found = checkActiveManager();
        if (found)
            System.out.println("manager has been found!\n");
        else {
            String value = "#! /bin/bash\n" + "wget --no-check-certificate --no-proxy https://jarsdt.s3.amazonaws.com/manager-1.0.jar\n" + "java -jar manager-1.0.jar\n";
            value = "#! /bin/bash\n"+"wget https://jarsdt.s3.amazonaws.com/manager-1.0.jar\n" + "java -jar manager-1.0.jar\n";
            try {
                String userdata = Base64.getEncoder()
                        .encodeToString(value.getBytes(StandardCharsets.UTF_8.toString()));
                AWSAbstractions.createEC2Instance(ec2, "manager", userdata);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length >= 3) {
            // arguments
            String path = System.getProperty("user.dir");
            String input = args[0];
            inputFile = path + "/src/" + input;
            String output = args[1];
            outputFile = new File(path + "/src/" + output);
            n = Integer.parseInt(args[2]);
            if (args.length == 4) {
                terminate = true;
            }
            localApplocationId = UUID.randomUUID().toString();
            ec2 = AWSAbstractions.getEC2Client();

            sqs = AWSAbstractions.getSQSClient();
            S3Client s3 = AWSAbstractions.getS3Client();
            System.out.println("local app with ID " + localApplocationId + " started:\n");

            initializedBuckets(s3);

            // find the manager
            activateManager(localApplocationId);

            // upload the input file to S3
            PutObjectResponse fileLocation = s3.putObject(PutObjectRequest.builder().bucket("inputdt").key(localApplocationId+".txt").build(), RequestBody.fromFile(new File(inputFile)));


            sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerSQS).messageBody("new job" + "\t" + n + "\t" + localApplocationId + "\t" + fileLocation).build());

            // if terminate = true thn send a message to the manager
            if (terminate) {
                try {
                    TimeUnit.SECONDS.sleep(30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerSQS).messageBody("terminate" + "\t" + localApplocationId).build());
            }
            // check if the process is DONE
            for (;;) {
//                System.out.println("local app waiting for a done message\n");
                List<Message> messages =
                        sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(managerLocalSQS).maxNumberOfMessages(1).build()).messages();
                if (!messages.isEmpty()) {
                    String[] message = messages.get(0).body().split("\t");
                    AWSAbstractions.deleteMessageFromQueue(managerLocalSQS, messages.get(0), sqs);
                    if (message[0].equals("done job")) {
                        InputStream sum = s3.getObject(GetObjectRequest.builder().bucket("outputdt").key(localApplocationId).build());
                        String text = new BufferedReader(
                                new InputStreamReader(sum, StandardCharsets.UTF_8)).lines()
                                .collect(Collectors.joining("\n"));
                        try {
                            // create html file
                            String html = " <!DOCTYPE html>\n" +
                                    "<html>\n" +
                                    "<head>\n" +
                                    "<title>Parsing Result</title>\n" +
                                    "</head>\n" +
                                    "<body>\n" +
                                    "\n" +
                                    "<h1>"+"Your result is:"+"</h1>\n" +
                                    "<p>"+text+"</p>\n" +
                                    "\n" +
                                    "</body>\n" +
                                    "</html> ";
                            FileWriter fd = new FileWriter(outputFile + ".html");
                            fd.write(html);
                            fd.close();
                            break; // break after
                        } catch (IOException e) {
                            System.out.println("An error occurred.");
                            exit(1);
                        }
                    }
                    else if (message[0].equals(("test1")) || message[0].equals(("test2")) || message[0].equals(("test3")) || message[0].equals(("test4"))) {
                        System.out.println(message[0]);
                    }
                }
            }

        } else {
            System.out.println("There are not enough arguments.");
        }
    }
}