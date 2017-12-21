import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.backends.backend_pdf import FigureCanvasPdf
#from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from astropy.utils.console import ProgressBar


def subplots(nrow, ncol, **kwargs):
    fig = Figure(**kwargs)
    axes = [[fig.add_subplot(nrow, ncol, i+ncol*j+1)
             for i in range(ncol)]
            for j in range(nrow)]
    return fig, np.array(axes, dtype=object)


def sampler(data, outprefix):    
    with ProgressBar(100) as bar:
        fig1, axes1 = subplots(10, 10, figsize=(10, 10))
        fig2, axes2 = subplots(10, 10, figsize=(10, 10))
        for i, (ax1, ax2) in enumerate(zip(axes1.ravel(), axes2.ravel())):
            bar.update()
            while True:
                idx = np.random.randint(len(data['x']))            
                img = data['imgs'][idx]
                if np.all(np.isfinite(img)):
                    break
            for ax in [ax1, ax2]:
                ax.set_xticks([])
                ax.set_yticks([])
                ax.set_title(str(idx))
            ax1.imshow(img, interpolation='None', rasterized=True)
            ax2.imshow(np.log(img), interpolation='None', rasterized=True)

    canvas1 = FigureCanvasPdf(fig1)
    fig1.set_tight_layout(True)
    canvas1.print_figure("{}_psf_samples.pdf".format(outprefix), dpi=300)

    canvas2 = FigureCanvasPdf(fig2)
    fig2.set_tight_layout(True)
    canvas2.print_figure("{}_psf_samples_log.pdf".format(outprefix), dpi=300)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    sampler(data, args.outprefix)
