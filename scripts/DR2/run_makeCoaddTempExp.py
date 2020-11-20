import os
import subprocess
import multiprocessing

root_dir = '/global/cscratch1/sd/descdm/DC2/DR2'
with open(os.path.join(root_dir, 'test_results',
                       'DR2_corrupted_warps.txt')) as fd:
    files = [_.strip() for _ in fd.readlines()]

commands = []
for item in files:
    tokens = os.path.basename(item).split('.')[0].split('-')
    band, tract, patch, visit = tokens[1:5]
    commands.append(f'(time makeCoaddTempExp.py /global/cscratch1/sd/descdm/DC2/DR2/repo --rerun dr2-calexp:replacement-warps --id filter={band} tract={tract} patch={patch} --selectId visit={visit}) >& logging/makeCoaddTempExp_{band}_{tract}_{patch}_{visit}.log')
    print(commands[-1])

def run_makeCoaddTempExp(command):
    subprocess.check_call(command, shell=True)

processes = 10
with multiprocessing.Pool(processes=processes) as pool:
    workers = []
    for command in commands[1:]:
        workers.append(pool.apply_async(subprocess.check_call,
                                        (command,), dict(shell=True)))
    pool.close()
    pool.join()
    _ = [worker.get() for worker in workers]
