import pickle
import numpy as np

from matplotlib.backends.backend_pdf import FigureCanvasPdf
from matplotlib.figure import Figure


def subplots(nrow, ncol, **kwargs):
    fig = Figure(**kwargs)
    axes = [[fig.add_subplot(nrow, ncol, i+ncol*j+1)
             for i in range(ncol)]
            for j in range(nrow)]
    return fig, np.array(axes, dtype=object)


def compare_psfs(data1, data2, outprefix):
    if len(data1['x']) != len(data2['x']):
        raise ValueError("len(data1) != len(data2)  !!!!")
    fig, axes = subplots(9, 9, figsize=(10, 10))
    for irow in range(9):
        for icol in range(0, 9, 3):
            while True:
                idx = np.random.randint(len(data1['x']))
                img1 = data1['imgs'][idx]
                img2 = data2['imgs'][idx]
                if np.all(np.isfinite(img1)) and np.all(np.isfinite(img2)):
                    break
            vmin = np.min(img1)
            vmax = np.max(img1)
            axes[irow][icol].imshow(img1, interpolation='None', rasterized=True, 
                                    vmin=vmin, vmax=vmax,
                                    cmap='inferno')
            axes[irow][icol+1].imshow(img2, interpolation='None', rasterized=True, 
                                      vmin=vmin, vmax=vmax,
                                      cmap='inferno')
            axes[irow][icol+2].imshow(img1-img2, interpolation='None', rasterized=True, 
                                      vmin=-0.1*vmax, vmax=0.1*vmax,
                                      cmap='seismic')
    for ax in axes.ravel():
        ax.set_xticks([])
        ax.set_yticks([])

    canvas = FigureCanvasPdf(fig)
    fig.set_tight_layout(True)
    canvas.print_figure("{}_psf_compare.pdf".format(outprefix), dpi=300)    


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile1", type=str, help="input pickle file")
    parser.add_argument("infile2", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data1 = pickle.load(open(args.infile1, 'rb'))
    data2 = pickle.load(open(args.infile2, 'rb'))
    compare_psfs(data1, data2, args.outprefix)
