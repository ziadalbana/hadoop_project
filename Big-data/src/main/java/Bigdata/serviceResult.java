package Bigdata;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Service;

import java.io.Serializable;


public class serviceResult implements Serializable {
      String service = "";
      String  Cpu = "" ;
      String  Disk ="";
      String Ram= "";
      String CpuMT= "";
      String DiskMT ="";
      String  RamMT= "";
      String CpuM = "";
      String DiskM = "";
      String  RamM = "";
      String count = "";
      String timeStamp ="";
      public  serviceResult(){}
      public  serviceResult( String service,String timeStamp,
    String  Cpu, String CpuM, String CpuMT,
    String  Disk,String DiskM,String DiskMT,
    String Ram,String  RamM, String  RamMT,
    String count){
          this.service=service;
          this.Cpu=Cpu;
          this.Disk=Disk;
          this.Ram=Ram;
          this.CpuM=CpuM;
          this.DiskM=DiskM;
          this.RamM=RamM;
          this.CpuMT=CpuMT;
          this.DiskMT=DiskMT;
          this.RamMT=RamMT;
          this.count=count;
          this.timeStamp=timeStamp;
      }
//    public String getTime(double time){
//        Date date = new java.util.Date((long) (time*1000L));
//        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:ss");
//        return sdf.format(date);
//    }

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

    public String getCpuM() {
        return CpuM;
    }

    public void setCpuM(String cpuM) {
        CpuM = cpuM;
    }

    public String getDiskM() {
        return DiskM;
    }

    public void setDiskM(String diskM) {
        DiskM = diskM;
    }

    public String getRamM() {
        return RamM;
    }

    public void setRamM(String ramM) {
        RamM = ramM;
    }

    @Override
    public String toString() {
        return "serviceResult{" +
                "service='" + service + '\'' +
                ", Cpu='" + Cpu + '\'' +
                ", Disk='" + Disk + '\'' +
                ", Ram='" + Ram + '\'' +
                ", CpuMT='" + CpuMT + '\'' +
                ", DiskMT='" + DiskMT + '\'' +
                ", RamMT='" + RamMT + '\'' +
                ", CpuM='" + CpuM + '\'' +
                ", DiskM='" + DiskM + '\'' +
                ", RamM='" + RamM + '\'' +
                ", count='" + count + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }
}
