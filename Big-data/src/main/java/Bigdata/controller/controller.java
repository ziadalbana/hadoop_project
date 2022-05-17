package Bigdata.controller;

import Bigdata.MapRudece;
import Bigdata.serviceResult;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@CrossOrigin
public class controller {
    MapRudece m=new MapRudece();
    @GetMapping("/getAnalysis/{startDate}/{endDate}")
    public List<serviceResult> Analysis(@PathVariable("startDate") String startDate, @PathVariable("endDate") String endDate){
        ArrayList<serviceResult> res=m.analysis(startDate,endDate);
        System.out.println(res.size());
        return res;
    }

}
