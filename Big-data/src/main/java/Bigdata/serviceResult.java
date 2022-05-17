package Bigdata;

import java.text.SimpleDateFormat;
import java.util.Date;

public class serviceResult {
      String service;
      String  Cpu;
      String  Disk;
      String Ram;
      String CpuMT;
      String DiskMT;
      String  RamMT;
      String count;
      public  serviceResult(String service,
    String  Cpu,
    String  Disk,
    String Ram,
    String CpuMT,
    String DiskMT,
    String  RamMT,
    String count){
          this.service=service;
          this.Cpu=Cpu.substring(0,6);
          this.Disk=Disk.substring(0,6);
          this.Ram=Ram.substring(0,6);
          this.CpuMT=getTime(Double.parseDouble(CpuMT));
          this.DiskMT=getTime(Double.parseDouble(DiskMT));
          this.RamMT=getTime(Double.parseDouble(RamMT));
          this.count=count;
      }
    public String getTime(double time){
        Date date = new java.util.Date((long) (time*1000L));
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:ss");
        return sdf.format(date);
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getCpu() {
        return Cpu;
    }

    public void setCpu(String cpu) {
        Cpu = cpu;
    }

    public String getDisk() {
        return Disk;
    }

    public void setDisk(String disk) {
        Disk = disk;
    }

    public String getRam() {
        return Ram;
    }

    public void setRam(String ram) {
        Ram = ram;
    }

    public String getCpuMT() {
        return CpuMT;
    }

    public void setCpuMT(String cpuMT) {
        CpuMT = cpuMT;
    }

    public String getDiskMT() {
        return DiskMT;
    }

    public void setDiskMT(String diskMT) {
        DiskMT = diskMT;
    }

    public String getRamMT() {
        return RamMT;
    }

    public void setRamMT(String ramMT) {
        RamMT = ramMT;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }
}
