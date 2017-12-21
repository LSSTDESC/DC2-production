import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure


def whisker(data, outprefix):
    # We'll reduce the ~350000 points to a manageable number by histogramming
    # second moments.
    # Need to cull any NaNs
    w = np.isfinite(data['Mxx'])
    w &= np.isfinite(data['Myy'])
    w &= np.isfinite(data['Mxy'])

    x, y = data['x'][w], data['y'][w]
    N, xedges, yedges = np.histogram2d(x, y, bins=50)
    Mxx, _, _ = np.histogram2d(x, y, weights=data['Mxx'][w], bins=[xedges, yedges])
    Myy, _, _ = np.histogram2d(x, y, weights=data['Myy'][w], bins=[xedges, yedges])
    Mxy, _, _ = np.histogram2d(x, y, weights=data['Mxy'][w], bins=[xedges, yedges])
    Mxx /= N
    Myy /= N
    Mxy /= N

    e1 = (Mxx - Myy)/(Mxx + Myy)
    e2 = 2*Mxy / (Mxx + Myy)
    beta = 0.5*np.arctan2(e2, e1)
    e = np.hypot(e1, e2)

    x = 0.5*(xedges[1:] + xedges[:-1])
    y = 0.5*(yedges[1:] + yedges[:-1])

    x, y = np.meshgrid(x, y)
    dx = e*np.cos(beta)
    dy = e*np.sin(beta)
    
    outfile = "{}_whisker.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        fig = Figure(figsize=(12, 10))
        ax = fig.add_subplot(111)
        quiverargs = dict(
            angles='xy',
            headlength=1e-10,
            headwidth=0,
            headaxislength=0,
            minlength=0,
            pivot='middle',
            scale_units='xy',
            width=0.002,
            scale=0.005
        )
        ax.quiver(x, y, dx, dy, **quiverargs) 
        
        fig.tight_layout()
        pdf.savefig(fig, dpi=100)
        

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    whisker(data, args.outprefix)
