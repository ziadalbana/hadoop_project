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
      String CpuM;
      String DiskM;
      String  RamM;
      String count;
      public  serviceResult(){}
      public  serviceResult( String service,
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

    public void combine(String[] s1, String[] s2){
         this.setService(s1[0]);
         this.setCpu(String.valueOf((Double.parseDouble(s1[1])+Double.parseDouble(s2[1]))/2));
         compareCpu(s1[2],s2[2],s1[3],s2[3]);
         this.setDisk(String.valueOf((Double.parseDouble(s1[4])+Double.parseDouble(s2[4]))/2));
         compareDisk(s1[5],s2[5],s1[6],s2[6]);
         this.setRam(String.valueOf((Double.parseDouble(s1[7])+Double.parseDouble(s2[7]))/2));
         compareRAM(s1[8],s2[8],s1[9],s2[9]);
         this.setCount(String.valueOf((Double.parseDouble(s1[10])+Double.parseDouble(s2[10]))));
    }

    public String toString() {
        return this.getService()+","
                +this.getCpu()+","
                +this.getCpuM()+","
                +this.getCpuMT()+","
                +this.getDisk()+","
                +this.getDiskM()+","
                +this.getDiskMT()+","
                +this.getRam()+","
                +this.getRamM()+","
                +this.getRamMT()+","
                +this.getCount();
    }

    private void compareCpu(String max1, String max2, String time1, String time2){
          if (max1.compareTo(max2)>=0) {
              this.setCpuM(max1);
              this.setCpuMT(time1);
          }
          else {
              this.setCpuM(max2);
              this.setCpuMT(time2);
          }
    }
    private void compareDisk(String max1, String max2, String time1, String time2){
        if (max1.compareTo(max2)>=0) {
            this.setDiskM(max1);
            this.setDiskMT(time1);
        }
        else {
            this.setDiskM(max2);
            this.setDiskMT(time2);
        }
    }
    private void compareRAM(String max1, String max2, String time1, String time2){
        if (max1.compareTo(max2)>=0) {
            this.setRamM(max1);
            this.setRamMT(time1);
        }
        else {
            this.setRamM(max2);
            this.setRamMT(time2);
        }
    }
}
