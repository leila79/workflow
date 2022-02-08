import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import java.io.File;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public class test {

    private static Datacenter[] datacenter;

    enum workflow_type {
        MONTAGE,
        CYBER_SHAKE,
        SIPHT,
        EPIGENOMICS
    }

    private static List<Vm> createVM(int userId, workflow_type type) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<Vm> list = new LinkedList<Vm>();
        int dw = 5;
        switch (type){
            case MONTAGE:
                dw = 8;
                break;
            case CYBER_SHAKE:
                dw = 4;
                break;
            case SIPHT:
                dw = 5;
                break;
            case EPIGENOMICS:
                dw = 7;
                break;
        }
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
            vm[i] = new Vm(datacenter[i].getId(), userId, mips1, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        for (int i = vms; i < 2 * vms; i++) {
            vm[i] = new Vm(datacenter[i].getId(), userId, mips2, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        for (int i = 2 * vms; i < 3 * vms; i++) {
            vm[i] = new Vm(datacenter[i].getId(), userId, mips3, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
    }

    public static void main(String[] args) {
        try {
            String daxPath = "E:\\workflowsim\\config\\dax\\CyberShake_500_1.xml";
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            ReplicaCatalog.init(file_system);
            WorkflowParser parser = new WorkflowParser(0, daxPath);
            parser.parse();
//            System.out.println(parser.getTaskList().get(parser.getTaskList().size()-1).toString());
//            System.out.println(parser.getTaskList().size());

        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }


}
