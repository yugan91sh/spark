select P_MFGR, LO_SHIPMODE, sum(LO_REVENUE)
from SSB.LINEORDER
         inner join SSB.PART
                    on LINEORDER.LO_PARTKEY = PART.P_PARTKEY
group by P_MFGR, LO_SHIPMODE