import os
import sqlite3
import pandas as pd

opsim_db_file = '/global/cfs/cdirs/descssim/DC2/minion_1016_desc_dithered_v4_trimmed.db'
with sqlite3.connect(opsim_db_file) as con:
    df = pd.read_sql('select * from summary where propId=54', con)
visits = set(df['obsHistID'])

failed_warp_visits_4565 = set([238642, 236478, 256304, 238629, 4565])

failed_warp_visits = set([227976, 206346, 236833, 235893, 200752, 236478,
                          238629, 238642, 256304, 4565, 235784, 206052,
                          52543, 191179, 243018])

print(visits.intersection(failed_warp_visits))
