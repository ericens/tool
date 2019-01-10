
---------------------------------------------hive-语句关联导致全表扫描---------------------------------------------

select k2.tag_id as tag_id
  ,bidder_id
  ,test
  ,date
from (
       select req_id
         ,bidder_id
         ,test
         ,from_unixtime(floor(ts/1000),'yyyyMMddHH') date
         ,data_date
       from ssp.ods_f_request
            lateral view explode(receivers) tmp as bidder_id
                                            where data_date>=2018122805
                                            and data_date<=2018122807
                                                           and from_unixtime(floor(ts/1000),'yyyyMMddHH') >= 2018122805
                                                                                                          and from_unixtime(floor(ts/1000),'yyyyMMddHH') <= 2018122807
     ) k1
  join ssp.ods_f_request_imp_info k2
    on k1.data_date = k2.data_date
      --------------------------------------------没有这两行导致全表扫描
       and k2.data_date>=2018122805
       and k2.data_date<=2018122807
      --------------------------------------------没有这两行导致全表扫描
       and k1.req_id = k2.req_id
group by tag_id,bidder_id,k1.req_id,k1.test,date;


---------------------------------------------hive-语句关联导致全表扫描---------------------------------------------
