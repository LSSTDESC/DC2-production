#!/usr/bin/env python

"""
Create a restricted sample of an HDF5 merged coadd catalog
that contains only the columns necessary to support the DPDD
columns exposed in the GCRCatalog DC2 reader
"""

import os
import sys

import pandas as pd

# The above list was generated 'generate_columns_to_keep'
# But we don't want to regenerate this each time we run this script
# so we cache the result here.
columns_to_keep = \
['base_Blendedness_abs_flux', 'base_ClassificationExtendedness_value',
 'base_PixelFlags_flag_bad', 'base_PixelFlags_flag_clipped',
 'base_PixelFlags_flag_crCenter', 'base_PixelFlags_flag_edge',
 'base_PixelFlags_flag_interpolatedCenter',
 'base_PixelFlags_flag_saturatedCenter',
 'base_PixelFlags_flag_suspectCenter', 'base_PsfFlux_area',
 'base_SdssCentroid_flag', 'base_SdssCentroid_x', 'base_SdssCentroid_xSigma',
 'base_SdssCentroid_y', 'base_SdssCentroid_ySigma', 'base_SdssShape_psf_xx',
 'base_SdssShape_psf_xy', 'base_SdssShape_psf_yy', 'coord_dec', 'coord_ra',
 'deblend_skipped', 'ext_shapeHSM_HsmSourceMoments_flag',
 'ext_shapeHSM_HsmSourceMoments_xx', 'ext_shapeHSM_HsmSourceMoments_xy',
 'ext_shapeHSM_HsmSourceMoments_yy', 'g_base_PsfFlux_flag',
 'g_base_PsfFlux_flux', 'g_base_PsfFlux_fluxSigma', 'g_base_SdssShape_flag',
 'g_base_SdssShape_psf_xx', 'g_base_SdssShape_psf_xy',
 'g_base_SdssShape_psf_yy', 'g_base_SdssShape_xx', 'g_base_SdssShape_xy',
 'g_base_SdssShape_yy', 'g_mag', 'g_mag_err', 'g_modelfit_CModel_flux',
 'g_modelfit_CModel_fluxSigma', 'i_base_PsfFlux_flag', 'i_base_PsfFlux_flux',
 'i_base_PsfFlux_fluxSigma', 'i_base_SdssShape_flag',
 'i_base_SdssShape_psf_xx', 'i_base_SdssShape_psf_xy',
 'i_base_SdssShape_psf_yy', 'i_base_SdssShape_xx', 'i_base_SdssShape_xy',
 'i_base_SdssShape_yy', 'i_mag', 'i_mag_err', 'i_modelfit_CModel_flux',
 'i_modelfit_CModel_fluxSigma', 'id', 'parent', 'r_base_PsfFlux_flag',
 'r_base_PsfFlux_flux', 'r_base_PsfFlux_fluxSigma', 'r_base_SdssShape_flag',
 'r_base_SdssShape_psf_xx', 'r_base_SdssShape_psf_xy',
 'r_base_SdssShape_psf_yy', 'r_base_SdssShape_xx', 'r_base_SdssShape_xy',
 'r_base_SdssShape_yy', 'r_mag', 'r_mag_err', 'r_modelfit_CModel_flux',
 'r_modelfit_CModel_fluxSigma', 'u_base_PsfFlux_flag', 'u_base_PsfFlux_flux',
 'u_base_PsfFlux_fluxSigma', 'u_base_SdssShape_flag',
 'u_base_SdssShape_psf_xx', 'u_base_SdssShape_psf_xy',
 'u_base_SdssShape_psf_yy', 'u_base_SdssShape_xx', 'u_base_SdssShape_xy',
 'u_base_SdssShape_yy', 'u_mag', 'u_mag_err', 'u_modelfit_CModel_flux',
 'u_modelfit_CModel_fluxSigma', 'y_base_PsfFlux_flag', 'y_base_PsfFlux_flux',
 'y_base_PsfFlux_fluxSigma', 'y_base_SdssShape_flag',
 'y_base_SdssShape_psf_xx', 'y_base_SdssShape_psf_xy',
 'y_base_SdssShape_psf_yy', 'y_base_SdssShape_xx', 'y_base_SdssShape_xy',
 'y_base_SdssShape_yy', 'y_mag', 'y_mag_err', 'y_modelfit_CModel_flux',
 'y_modelfit_CModel_fluxSigma', 'z_base_PsfFlux_flag', 'z_base_PsfFlux_flux',
 'z_base_PsfFlux_fluxSigma', 'z_base_SdssShape_flag',
 'z_base_SdssShape_psf_xx', 'z_base_SdssShape_psf_xy',
 'z_base_SdssShape_psf_yy', 'z_base_SdssShape_xx', 'z_base_SdssShape_xy',
 'z_base_SdssShape_yy', 'z_mag', 'z_mag_err', 'z_modelfit_CModel_flux',
 'z_modelfit_CModel_fluxSigma']


def generate_columns_to_keep():
    import GCRCatalogs
    gc = GCRCatalogs.load('dc2_coadd_run1.1p_tract4850')

    columns_to_keep = []
    for k, v in gc.quantity_modifiers.items():
        if isinstance(v, str):
            columns_to_keep.append(v)
        else:  # assume it's an iterable of stings
            for vi in v:
                if isinstance(vi, str):
                    columns_to_keep.append(vi)

    columns_to_keep = list(set(columns_to_keep))
    return columns_to_keep


def load_trim_save_patch(infile, outfile, patch, key_prefix='coadd',
                         verbose=False):
    import re
    r = re.search('merged_tract_([0-9]+)\.', infile)
    tract = int(r[1])

    key = "%s_%s_%s" % (key_prefix, tract, patch)
    try:
        df = pd.read_hdf(infile, key=key)
    except KeyError as e:
        if verbose:
            print(e)
        return

    columns_to_keep_present = list(set(columns_to_keep).intersection(df.columns))
    trim_df = df[columns_to_keep_present]
    trim_df.to_hdf(outfile, key=key)


def make_trim_file(infile, outfile=None):
    # Note '%d%d' instead of '%d,%d'
    nx, ny = 8, 8
    patches = ['%d%d' % (i, j) for i in range(nx) for j in range(ny)]

    if outfile is None:
        dirname = os.path.dirname(infile)
        basename = os.path.basename(infile)
        outfile = os.path.join(dirname, "trim_"+basename)

    for patch in patches:
        load_trim_save_patch(infile, outfile, patch)


if __name__ == "__main__":
    for infile in sys.argv[1:]:
        make_trim_file(infile)
