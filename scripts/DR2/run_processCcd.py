import os
import glob
import subprocess
import multiprocessing
import sqlite3
import pandas as pd

class RunProcessCcd:
    """
    Callback function class for running processCcd.py in a subprocess
    via the multiprocessing module.
    """
    def __init__(self, repo, rerun, logging_dir):
        self.template = 'processCcd.py {} --rerun {} --id visit=%(visit)d raftName=%(raftName)s detectorName=%(detectorName)s'.format(repo, rerun)
        self.logging_dir = logging_dir

    def __call__(self, visit, filt, raftName, detectorName):
        det_name = '_'.join((raftName, detectorName))
        command = self.template % locals()
        log_file = os.path.join(self.logging_dir,
                                f'processCcd_{visit}-{filt}_{det_name}.log')

        full_command = f"(time {command}) >& {log_file}"
        print(full_command)
        subprocess.check_call(full_command, shell=True)

def run_processCcd_pool(visits, repo, rerun, processes, logging_dir):
    os.makedirs(logging_dir, exist_ok=True)
    registry = os.path.join(repo, 'registry.sqlite3')
    with sqlite3.connect(registry) as conn:
        exp_info = pd.read_sql('select visit, filter, raftName, detectorName, '
                               'detector from raw', conn)
    run_processCcd = RunProcessCcd(repo, rerun, logging_dir)
    with multiprocessing.Pool(processes=processes) as pool:
        results = []
        for visit in visits:
            df = exp_info.query('visit=={}'.format(visit))
            print(visit, len(df))
            for irow in range(len(df)):
                row = df.iloc[irow]
                filt = row['filter']
                raftName = row['raftName']
                detectorName = row['detectorName']
                visit_dir = f'{visit:08d}-{filt}'
                suffix = f'det{row["detector"]:03d}.fits'
                calexp_fn = 'calexp_' + '-'.join((visit_dir, raftName,
                                                  detectorName, suffix))
                calexp_path = os.path.join(repo, 'rerun', rerun, 'calexp',
                                           visit_dir, raftName, calexp_fn)
                if not os.path.isfile(calexp_path):
                    results.append(pool.apply_async(run_processCcd,
                                                    (visit, filt, raftName,
                                                     detectorName)))
        pool.close()
        pool.join()
        for res in results:
            res.get()


if __name__ == '__main__':
    import sys
    import configparser
    cp = configparser.ConfigParser()
    cp.optionxform = str

    cp.read(sys.argv[1])
    config = dict(cp.items('DEFAULT'))
    repo = config['repo']
    rerun = config['rerun']
    logging_dir = config['logging_dir']
    if config['visits'] == 'all':
        visits = [int(os.path.basename(_)) for _ in
                  glob.glob(os.path.join(repo, 'raw', '*'))]
    else:
        visits = [int(_.strip()) for _ in config['visits'].split(',')]
    print(visits)
    processes = int(config['processes'])
    print(config)
    run_processCcd_pool(visits, repo, rerun, processes, logging_dir)
