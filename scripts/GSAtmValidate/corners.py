import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
import corner


def getRange(data):
    w = np.isfinite(data)
    ret = np.percentile(data[w], (1.0, 99.0))
    span = ret[1] - ret[0]
    ret += 0.3*np.array([-span, span])
    return tuple(ret)


def corners(data, outprefix):
    keys = ['x', 'y', 'e1', 'e2', 'e', 'beta', 'rsqr', 'Mxx', 'Myy', 'Mxy']

    cornerdata = np.full((len(data['x']), len(keys)), np.nan, dtype=float)
    ranges = []
    for ikey, key in enumerate(keys):
        cornerdata[:, ikey] = data[key]
        ranges.append(getRange(data[key]))

    outfile = "{}_corner.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        fig = corner.corner(cornerdata, range=ranges, labels=keys)
        pdf.savefig(fig, dpi=100)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    corners(data, args.outprefix)
