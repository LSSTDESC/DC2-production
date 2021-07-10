import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
import pymaster as nmt

class FlatMapInfo(object) :
    def __init__(self,x_range,y_range,nx=None,ny=None,dx=None,dy=None) :
        """
        Creates a flat map
        x_range : [x_i,x_f] range in the x axis covered by the map
        y_range : [y_i,y_f] range in the y axis covered by the map
        nx,ny : Number of pixels in the x/y axes. If None, dx/dy must be provided
        dx,dy : Resolution in the x/y axes. If None, nx/ny must be provided
        """
        self.x0=x_range[0]
        self.xf=x_range[1]
        self.lx=self.xf-self.x0
        self.y0=y_range[0]
        self.yf=y_range[1]
        self.ly=self.yf-self.y0
        
        if nx is None and dx is None :
            raise ValueError("Must provide either nx or dx")

        if ny is None and dy is None :
            raise ValueError("Must provide either ny or dy")

        if nx is None :
            self.nx=int(self.lx/dx)+1
        else :
            self.nx=nx
        self.dx=self.lx/self.nx

        if ny is None :
            self.ny=int(self.ly/dy)+1
        else :
            self.ny=ny
        self.dy=self.ly/self.ny

        self.npix=self.nx*self.ny

    def get_dims(self) :
        """
        Returns map size
        """
        return [self.ny,self.nx]
        
    def get_size(self) :
        """
        Returns map size
        """
        return self.npix

    def pos2pix(self,x,y) :
        """
        Returns pixel indices for arrays of x and y coordinates.
        Will return -1 if (x,y) lies outside the map
        """
        x=np.asarray(x)
        scalar_input=False
        if x.ndim==0 :
            x=x[None]
            scalar_input=True

        y=np.asarray(y)
        if y.ndim==0 :
            y=y[None]

        if len(x)!=len(y) :
            raise ValueError("x and y must have the same size!")

        ix=np.floor((x-self.x0)/self.dx).astype(int)
        ix_out=np.where(np.logical_or(ix<0,ix>=self.nx))[0]

        iy=np.floor((y-self.y0)/self.dy).astype(int)
        iy_out=np.where(np.logical_or(iy<0,iy>=self.ny))[0]

        ipix=ix+self.nx*iy
        ipix[ix_out]=-1
        ipix[iy_out]=-1

        if scalar_input :
            return np.squeeze(ipix)
        return ipix

    def pix2pos(self,ipix) :
        """
        Returns x,y coordinates of pixel centres for a set of pixel indices.
        """
        ipix=np.asarray(ipix)
        scalar_input=False
        if ipix.ndim==0 :
            ipix=ipix[None]
            scalar_input=True

        i_out=np.where(np.logical_or(ipix<0,ipix>=self.npix))[0]
        if len(i_out)>0 : 
            raise ValueError("Pixels outside of range")

        ix=ipix%self.nx
        ioff=np.array(ipix-ix)
        iy=ioff.astype(int)/(int(self.nx))

        x=self.x0+(ix+0.5)*self.dx
        y=self.y0+(iy+0.5)*self.dy

        if scalar_input :
            return np.squeeze(x),np.squeeze(y)
        return x,y

    def get_empty_map(self) :
        """
        Returns a map full of zeros
        """
        return np.zeros(self.npix,dtype=float)

    def view_map(self,map_in,ax=None, xlabel='x', ylabel='y',
		 title=None, addColorbar=True,posColorbar= False, cmap = cm.magma,
                 colorMax= None, colorMin= None):
        """
        Plots a 2D map (passed as a flattened array)
        """
        if len(map_in)!=self.npix :
            raise ValueError("Input map doesn't have the correct size")

	# set up the colorbar if min, max not given.
        if colorMax is None or colorMin is None:
            if posColorbar:
                ind= np.where(map_in>0)[0]
                colorMin= np.percentile(map_in[ind], 15)
                colorMax= np.percentile(map_in[ind], 95)
            else:
                colorMin= np.percentile(map_in, 15)
                colorMax= np.percentile(map_in, 95)

        if ax is None :
            plt.figure()
            ax=plt.gca()
        if title is not None :
            ax.set_title(title,fontsize=15)
        image= ax.imshow(map_in.reshape([self.ny,self.nx]),
			origin='lower', interpolation='nearest',
			aspect='equal', extent=[self.x0,self.xf,self.y0,self.yf],
			vmin= colorMin, vmax= colorMax, cmap= cmap)
        #if addColorbar :
	#    plt.colorbar(image)
        ax.set_xlabel(xlabel,fontsize=15)
        ax.set_ylabel(ylabel,fontsize=15)

    def write_flat_map(self,filename,maps) :
        """
        Saves a set of maps in npz format.
        We'll try to implement other more standard formats with proper WCS coordinates etc. ASAP.
        """
        if maps.ndim<1 :
            raise ValueError("Must supply at least one map")
        if maps.ndim==1 :
            maps=np.array([maps])
        if len(maps[0])!=self.npix :
            raise ValueError("Map doesn't conform to this pixelization")
            
        np.savez(filename,x_range=[self.x0,self.xf],y_range=[self.y0,self.yf],nx=self.nx,ny=self.ny,
                 maps=maps)

    def compute_power_spectrum(self,map1,mask1,map2=None,mask2=None,l_bpw=None,return_bpw=False,wsp=None,return_wsp=False) :
        """
        Computes power spectrum between two maps.
        map1 : first map to correlate
        mask1 : mask for the first map
        map2 : second map to correlate. If None map2==map1.
        mask2 : mask for the second map. If None mask2==mask1.
        l_bpw : bandpowers on which to calculate the power spectrum. Should be an [2,N_ell] array, where
                the first and second columns list the edges of each bandpower. If None, the function will
                create bandpowers of its own taylored to the properties of your map.
        return_bpw : if True, the bandpowers will also be returned
        wsp : NmtWorkspaceFlat object to accelerate the calculation. If None, this will be precomputed.
        return_wsp : if True, the workspace will also be returned 
        """
        same_map=False
        if map2 is None :
            map2=map1
            same_map=True
        
        same_mask=False
        if mask2 is None :
            mask2=mask1
            same_mask=False

        if len(map1)!=self.npix :
            raise ValueError("Input map has the wrong size")
        if (len(map1)!=len(map2)) or (len(map1)!=len(mask1)) or (len(map1)!=len(mask2)) :
            raise ValueError("Sizes of all maps and masks don't match")
            
        lx_rad=self.lx*np.pi/180
        ly_rad=self.ly*np.pi/180

        if l_bpw is None :
            ell_min=max(2*np.pi/lx_rad,2*np.pi/ly_rad)
            ell_max=min(self.nx*np.pi/lx_rad,self.ny*np.pi/ly_rad)
            d_ell=2*ell_min
            n_ell=int((ell_max-ell_min)/d_ell)-1
            l_bpw=np.zeros([2,n_ell])
            l_bpw[0,:]=ell_min+np.arange(n_ell)*d_ell
            l_bpw[1,:]=l_bpw[0,:]+d_ell
            return_bpw=True
        
        #Generate binning scheme
        b=nmt.NmtBinFlat(l_bpw[0,:],l_bpw[1,:])

        #Generate fields
        f1=nmt.NmtFieldFlat(lx_rad,ly_rad,mask1.reshape([self.ny,self.nx]),[map1.reshape([self.ny,self.nx])])
        if same_map and same_mask :
            f2=f1
        else :
            f2=nmt.NmtFieldFlat(lx_rad,ly_rad,mask2.reshape([self.ny,self.nx]),[map2.reshape([self.ny,self.nx])])

        #Compute workspace if needed
        if wsp is None :
            wsp=nmt.NmtWorkspaceFlat();
            wsp.compute_coupling_matrix(f1,f2,b)
            return_wsp=True

        #Compute power spectrum
        cl_coupled=nmt.compute_coupled_cell_flat(f1,f2)
        cl_uncoupled=wsp.decouple_cell(cl_coupled)[0]

        #Return
        if return_bpw and return_wsp :
            return cl_uncoupled,l_bpw,wsp
        else :
            if return_bpw :
                return cl_uncoupled,l_bpw
            elif return_wsp :
                return cl_uncoupled,wsp
            else :
                return cl_uncoupled

    def u_grade(self,mp,x_fac,y_fac=None) :
        """
        Up-grades the resolution of a map and returns the associated FlatSkyInfo object.
        mp : input map
        x_fac : the new map will be sub-divided into x_fac*nx pixels in the x direction
        y_fac : the new map will be sub-divided into y_fac*ny pixels in the y direction
                if y_fac=None, then y_fac=x_fac
        """
        if y_fac is None :
            y_fac=x_fac
        if len(mp)!=self.npix :
            raise ValueError("Input map has a wrong size")

        fm_ug=FlatMapInfo([self.x0,self.xf],[self.y0,self.yf],nx=x_fac*self.nx,ny=y_fac*self.ny)
        mp_ug=np.repeat(np.repeat(mp.reshape([self.ny,self.nx]),y_fac,axis=0),x_fac,axis=1).flatten()
        
        return fm_ug,mp_ug

    def d_grade(self,mp,x_fac,y_fac=None) :
        """
        Down-grades the resolution of a map and returns the associated FlatSkyInfo object.
        mp : input map
        x_fac : the new map will be sub-divided into floor(nx/x_fac) pixels in the x direction
        y_fac : the new map will be sub-divided into floor(ny/y_fac) pixels in the y direction
                if y_fac=None, then y_fac=x_fac.
        Note that if nx/ny is not a multiple of x_fac/y_fac, the remainder pixels will be lost.
        """
        if y_fac is None :
            y_fac=x_fac
        if len(mp)!=self.npix :
            raise ValueError("Input map has a wrong size")

        nx_new=self.nx/int(x_fac)
        ny_new=self.ny/int(y_fac)
        xf_new=self.x0+self.dx*x_fac*nx_new
        yf_new=self.y0+self.dy*y_fac*ny_new

        ix_max=nx_new*int(x_fac)
        iy_max=ny_new*int(y_fac)
        mp2d=mp.reshape([self.ny,self.nx])[:iy_max,:ix_max]
        fm_dg=FlatMapInfo([self.x0,xf_new],[self.y0,yf_new],nx=nx_new,ny=ny_new)
        mp_dg=np.mean(np.mean(np.reshape(mp2d.flatten(),[ny_new,int(y_fac),nx_new,int(x_fac)]),axis=-1),axis=-2).flatten()
        
        return fm_dg,mp_dg

def read_flat_map(filename,i_map=0) :
    """
    Reads a flat-sky map and the details of its pixelization scheme.
    The latter are returned as a FlatMapInfo object.
    i_map : map to read. If -1, all maps will be read.
    """
    data=np.load(filename)

    fmi=FlatMapInfo(data['x_range'],data['y_range'],nx=data['nx'],ny=data['ny'])
    if i_map==-1 :
        i_map=np.arange(len(data['maps']))
    return fmi,data['maps'][i_map]
