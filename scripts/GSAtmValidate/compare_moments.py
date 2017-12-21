import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from corner import hist2d


def compare_moments(data1, data2, outprefix):
    if len(data1['x']) != len(data2['x']):
        raise ValueError("len(data1) != len(data2)  !!!!")

    keys = ['e1', 'e2', 'e', 'beta', 'rsqr', 'Mxx', 'Myy', 'Mxy']
    
    outfile = "{}_compare_moments.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        for key in keys:
            fig = Figure(figsize=(12, 10))
            ax = fig.add_subplot(111)
            w = np.isfinite(data1[key])
            w &= np.isfinite(data2[key])
            hist2d(data1[key][w], data2[key][w], ax=ax)
            ax.set_title(key)
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            lim = min(xlim[0], ylim[0]), max(xlim[1], ylim[1])
            ax.plot(lim, lim, c='r')
            ax.set_xlim(lim)
            ax.set_ylim(lim)
            fig.tight_layout()
            pdf.savefig(fig, dpi=100)
        


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile1", type=str, help="input pickle file")
    parser.add_argument("infile2", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data1 = pickle.load(open(args.infile1, 'rb'))
    data2 = pickle.load(open(args.infile2, 'rb'))
    compare_moments(data1, data2, args.outprefix)

