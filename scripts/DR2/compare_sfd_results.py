import os
import glob
import matplotlib.pyplot as plt
import pandas as pd

def get_num_ccds(repo, visit):
    return len(glob.glob(os.path.join(repo, 'calexp', f'{visit:08d}-?',
                                      'R*', 'calexp*.fits')))

bands = 'ugrizy'

srs_results = pd.concat([pd.read_pickle(_) for _ in
                         glob.glob('srs_sfp_results/sfp_metrics*.pkl')],
                        ignore_index=True)

run31_results = pd.concat([pd.read_pickle(_) for _ in
                           glob.glob('run3.1i_sfp_results/sfp_metrics*.pkl')],
                          ignore_index=True)
run31_results['num_ccds'] = [get_num_ccds('repo/rerun/calexp-dr2', visit)
                             for visit in run31_results['visit']]

df = run31_results.set_index('visit').\
     join(srs_results.set_index('visit'), lsuffix='_run31', rsuffix='_run21')
df['dm5'] = df['m5_run31'] - df['m5_run21']

fig = plt.figure(figsize=(9, 6))
columns = ['ast_offset', 'dmag_ref_median', 'dmag_calib_median', 'T_median',
           'm5']
for i, column in enumerate(columns, 1):
    fig.add_subplot(2, 3, i)
    for band in bands:
        my_df = df.query(f'band_run31 == "{band}"')
        xcol = f'{column}_run21'
        ycol = f'{column}_run31'
        plt.scatter(my_df[xcol], my_df[ycol] - my_df[xcol], s=2, label=band)
        #plt.scatter(my_df[xcol], my_df[ycol], s=2, label=band)
        plt.xlabel(xcol)
        plt.ylabel(f'{ycol} - {xcol}')
    plt.legend(fontsize='x-small')
plt.tight_layout()

plt.savefig('sfp_comparison_dr2_run3.1_vs_cc_in2p3.png')
