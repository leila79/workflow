import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;

public class FCFS_Scheduler {

    private static List<Task> cloudletList;
    private static List<Vm> vmList;
    private static Datacenter datacenter;
    private static double[][] commMatrix;
    private static double[][] execMatrix;

    enum workflow_type {
        MONTAGE,
        CYBER_SHAKE,
        SIPHT,
        EPIGENOMICS
    }

    private static List<Vm> createVM(int userId, workflow_type type) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<Vm> list = new LinkedList<Vm>();

        int dw = switch (type) {
            case MONTAGE -> 8;
            case CYBER_SHAKE -> 4;
            case SIPHT -> 5;
            case EPIGENOMICS -> 7;
        };
        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips1 = 500;
        int mips2 = 1000;
        int mips3 = 1500;
        long bw = 1000;
        int nw = 500;
        int vms = (int) Math.ceil(nw / (10 * dw));
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        Vm[] vm = new Vm[vms * 3];

        for (int i = 0; i < vms; i++) {
            vm[i] = new Vm(datacenter.getId(), userId, mips1, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        for (int i = vms; i < 2 * vms; i++) {
            vm[i] = new Vm(datacenter.getId(), userId, mips2, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        for (int i = 2 * vms; i < 3 * vms; i++) {
            vm[i] = new Vm(datacenter.getId(), userId, mips3, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
    }

    private static List<Task> createTasks(int userId, String daxPath) {
        // Creates a container to store Cloudlets
//        List<Task> list = new List<Task>();

        java.io.File daxFile = new File(daxPath);
        if (!daxFile.exists()) {
            Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
//            return list;
        }
        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
        ReplicaCatalog.init(file_system);
        WorkflowParser parser = new WorkflowParser(userId, daxPath);
        parser.parse();
//        Collections.copy(list, parser.getTaskList());
        return parser.getTaskList();
    }

    public static Datacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more Machines
        List<Host> hostList = new ArrayList<Host>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating a Machine.
        List<Pe> peList = new ArrayList<Pe>();

        int mips = 1000;

        // 3. Create PEs and add these into the list.
        peList.add(new Pe(0, new PeProvisionerSimple(mips)));

        //4. Create Hosts with its id and list of PEs and add them to the list of machines
        int hostId = 0;
        int ram = 2048; //host memory (MB)
        long storage = 1000000; //host storage
        int bw = 10000;

        hostList.add(
                new Host(
                        hostId,
                        new RamProvisionerSimple(ram),
                        new BwProvisionerSimple(bw),
                        storage,
                        peList,
                        new VmSchedulerTimeShared(peList)
                )
        ); // This is our first machine


        // 5. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;        // the cost of using memory in this resource
        double costPerStorage = 0.1;    // the cost of using storage in this resource
        double costPerBw = 0.1;            // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>();    //we are not adding SAN devices by now

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


        // 6. Finally, we need to create a PowerDatacenter object.
        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    public static void main(String[] args) {
        Log.printLine("Starting FCFS Scheduler...");

//        new GenerateMatrices();
//        execMatrix = GenerateMatrices.getExecMatrix();
//        commMatrix = GenerateMatrices.getCommMatrix();

        try {
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            CloudSim.init(num_user, calendar, trace_flag);
            int NO_OF_DATA_CENTERS = 5;
            // Second step: Create Datacenters
            datacenter = createDatacenter("datacenter_0");


            //Third step: Create Broker
            FCFSDatacenterBroker broker = createBroker("Broker_0");
            int brokerId = broker.getId();

            //Fourth step: Create VMs and Cloudlets and send them to broker
//            workflow_type type = workflow_type.MONTAGE;
            vmList = createVM(brokerId, workflow_type.CYBER_SHAKE);
            cloudletList = createTasks(brokerId,"E:\\workflowsim\\config\\dax\\CyberShake_500_1.xml");

            broker.submitVmList(vmList);
            broker.submitCloudletList(cloudletList);

            // Fifth step: Starts the simulation
            CloudSim.startSimulation();

            // Final step: Print results when simulation is over
            List<Cloudlet> newList = broker.getCloudletReceivedList();
            //newList.addAll(globalBroker.getBroker().getCloudletReceivedList());

            CloudSim.stopSimulation();

            printCloudletList(newList);

            Log.printLine(FCFS_Scheduler.class.getName() + " finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    private static FCFSDatacenterBroker createBroker(String name) throws Exception {
        return new FCFSDatacenterBroker(name);
    }

    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" +
                indent + "Data center ID" +
                indent + "VM ID" +
                indent + indent + "Time" +
                indent + "Start Time" +
                indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        dft.setMinimumIntegerDigits(2);
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            Log.print(indent + dft.format(cloudlet.getCloudletId()) + indent + indent);

            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + dft.format(cloudlet.getResourceId()) +
                        indent + indent + indent + dft.format(cloudlet.getVmId()) +
                        indent + indent + dft.format(cloudlet.getActualCPUTime()) +
                        indent + indent + dft.format(cloudlet.getExecStartTime()) +
                        indent + indent + indent + dft.format(cloudlet.getFinishTime()));
            }
        }
//        double makespan = calcMakespan(list);
//        Log.printLine("Makespan using FCFS: " + makespan);
    }

//    private static double calcMakespan(List<Cloudlet> list) {
//        double makespan = 0;
//        double[] dcWorkingTime = new double[Constants.NO_OF_DATA_CENTERS];
//
//        for (int i = 0; i < Constants.NO_OF_TASKS; i++) {
//            int dcId = list.get(i).getVmId() % Constants.NO_OF_DATA_CENTERS;
//            if (dcWorkingTime[dcId] != 0) --dcWorkingTime[dcId];
//            dcWorkingTime[dcId] += execMatrix[i][dcId] + commMatrix[i][dcId];
//            makespan = Math.max(makespan, dcWorkingTime[dcId]);
//        }
//        return makespan;
//    }
}
