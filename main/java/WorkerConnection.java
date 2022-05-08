import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import tools.AWSAbstractions;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WorkerConnection implements Runnable {


    public void run() {
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(60);
            } catch (InterruptedException e) {
                if (AWSAbstractions.numOfAllJobs() == 0) break;
            }
            Ec2Client ec2 = AWSAbstractions.getEC2Client();
            DescribeInstancesRequest req = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse res = ec2.describeInstances(req);
            for (Reservation reservation : res.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.tags().isEmpty())
                        continue;
                    String name = instance.tags().get(0).value();
                    String state = instance.state().name().toString();
                    if (name.equals("worker")) {
                        if (state.equals("shutting-down") || state.equals("terminated") || state.equals("stopping") || state.equals("stopped")) {
                            AWSAbstractions.reassignTasks(instance.instanceId());    // reassign the tasks to other workers
                        }
                    }
                }
            }
            if (Thread.currentThread().isInterrupted() && AWSAbstractions.numOfAllJobs() == 0)
                break;
        }
    }
}