import pandas as pd

df = pd.read_pickle('protoDC2_visits.pkl')

for filt in 'ugrizy':
    df_filt = df.query('filter=="%s"' % filt)
    df_filt.sort_values('expMJD')
    with open('protoDC2_visits_%s-band.txt' % filt, 'w') as output:
        for irow in df_filt.index:
            output.write('%i  %s\n' % (df_filt.loc[irow]['obsHistID'],
                                       df_filt.loc[irow]['expMJD']))
