import os
import glob
import subprocess
import multiprocessing
import lsst.daf.persistence as dp


log_dir = 'logging'


def sfp_outfile(outdir, visit, band):
    return f'{outdir}/sfp_metrics_v{visit}-{band}.pkl'


def run_sfp_validation(repo, visit, opsim_db_file, outdir, log_dir=log_dir,
                       log_suffix=''):
    print(f"working on visit {visit}")
    command = (f'(time make_sfp_validation_plots.py {repo} {visit} '
               f'--opsim_db {opsim_db_file} --outdir {outdir}) >& '
               f'{log_dir}/make_sfp_validation_{visit}{log_suffix}.log')
    return subprocess.check_call(command, shell=True)


def find_processed_visits(repo):
    visits = []
    for item in glob.glob(os.path.join(repo, 'calexp', '*-?')):
        visit, band = os.path.basename(item).split('-')
        visit = int(visit)
        visits.append((band, visit))
    return visits


run31i_repo = 'repo/rerun/dr2-calexp'

repo = '/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1'
outdir = 'srs_sfp_results'
log_suffix = '_run2.2i'

#repo = run31i_repo
#outdir = 'run3.1i_sfp_results'
#log_suffix = 'run3.1i'

# Just process the Run3.1i visits, even for the Run2.2i data.
visits = find_processed_visits(run31i_repo)

opsim_db_file = '/global/cfs/cdirs/descssim/DC2/minion_1016_desc_dithered_v4_trimmed.db'

processes = 5
with multiprocessing.Pool(processes=processes) as pool:
    workers = []
    for band, visit in visits:
        if os.path.isfile(sfp_outfile(outdir, visit, band)):
            continue
        workers.append(pool.apply_async(run_sfp_validation,
                                        (repo, visit, opsim_db_file, outdir),
                                        dict(log_suffix=log_suffix)))
    pool.close()
    pool.join()
    _ = [worker.get() for worker in workers]
