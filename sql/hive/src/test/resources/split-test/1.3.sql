select P_MFGR, LO_SHIPMODE, sum(LO_REVENUE)
from SSB_P.LINEORDER
         inner join SSB_P.PART
                    on LINEORDER.LO_PARTKEY = PART.P_PARTKEY
group by P_MFGR, LO_SHIPMODE