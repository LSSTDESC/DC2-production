import pickle
from sampler import sampler
from corners import corners
from xi import xi
from hexbins import hexbins
from whisker import whisker



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    
    for proc in [sampler, corners, xi, hexbins, whisker]:
        proc(data, args.outprefix)
