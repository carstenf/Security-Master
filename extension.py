# general
import pandas as pd
from zipline.data.bundles import register

from zipline.data.bundles.db_to_bundle import sec_master_q
register(
     'sep',
      sec_master_q,
      calendar_name='NYSE',
      ) 
      




