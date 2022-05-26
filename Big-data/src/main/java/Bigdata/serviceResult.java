package Bigdata;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Service;

import java.io.Serializable;


public class serviceResult implements Serializable {

      String service = "";
      Double  Cpu=0.0;
      Double  Disk=0.0;
      Double Ram=0.0;
      Double CpuMT=0.0;
      Double DiskMT=0.0;
      Double  RamMT=0.0;
      Double CpuM =0.0;
      Double DiskM=0.0;
      Double  RamM=0.0;
      Double count=0.0;
      String timeStamp ="";

      public  serviceResult(){}
      public  serviceResult( String service,String timeStamp,
                             double  Cpu, double CpuM, double CpuMT,
                             double  Disk,double DiskM,double DiskMT,
                             double Ram,double  RamM, double  RamMT,
                             double count){
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

    public Double getCpu() {
        return Cpu;
    }

    public void setCpu(Double cpu) {
        Cpu = cpu;
    }

    public Double getDisk() {
        return Disk;
    }

    public void setDisk(Double disk) {
        Disk = disk;
    }

    public Double getRam() {
        return Ram;
    }

    public void setRam(Double ram) {
        Ram = ram;
    }

    public Double getCpuMT() {
        return CpuMT;
    }

    public void setCpuMT(Double cpuMT) {
        CpuMT = cpuMT;
    }

    public Double getDiskMT() {
        return DiskMT;
    }

    public void setDiskMT(Double diskMT) {
        DiskMT = diskMT;
    }

    public Double getRamMT() {
        return RamMT;
    }

    public void setRamMT(Double ramMT) {
        RamMT = ramMT;
    }

    public Double getCpuM() {
        return CpuM;
    }

    public void setCpuM(Double cpuM) {
        CpuM = cpuM;
    }

    public Double getDiskM() {
        return DiskM;
    }

    public void setDiskM(Double diskM) {
        DiskM = diskM;
    }

    public Double getRamM() {
        return RamM;
    }

    public void setRamM(Double ramM) {
        RamM = ramM;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
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
