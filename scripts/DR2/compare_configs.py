def get_entries(config_file, strip_processCcd=True):
    entries = []
    with open(config_file) as fd:
        for line in fd:
            if not line.startswith('config'):
                continue
            entry = line.strip()
            if strip_processCcd:
                entry = '.'.join([_ for _ in entry.split('.') if _ != 'processCcd'])
            entries.append(entry)
    return set(entries)

dr2_run31 = get_entries('repo/rerun/dr2-calexp/config/processCcd.py')

cc_in2p3 = get_entries('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1/config/singleFrameDriver.py')

def run_eval(expression):
    key, value = expression.split('=')
    key = key.replace('.', '_')
    value = eval(value)
    if isinstance(value, list):
        value = set(value)
    return {key: value}

dr2_sets = dict()
for item in dr2_run31.difference(cc_in2p3):
    dr2_sets.update(run_eval(item))

cc_in2p3_sets = dict()
for item in cc_in2p3.difference(dr2_run31):
    cc_in2p3_sets.update(run_eval(item))

for key, value in dr2_sets.items():
    try:
        assert(cc_in2p3_sets[key] == value)
    except AssertionError:
        print(key, value)
    except KeyError as eobj:
        print('not in cc_in2p3 config:')
        print(key, value)

for key, value in cc_in2p3_sets.items():
    try:
        assert(dr2_sets[key] == value)
    except AssertionError:
        print(key, value)
    except KeyError as eobj:
        print('not in dr2 config:')
        print(key, value)

