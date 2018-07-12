#!/usr/bin/env python

"""
Create a restricted sample of an HDF5 merged coadd catalog
that contains only the columns necessary to support the DPDD
columns exposed in the GCRCatalog DC2 reader
"""


import pandas as pd

columns_to_keep = \
['id', 'parent', 'coord_ra', 'coord_dec', 'base_SdssCentroid_x',
'base_SdssCentroid_y', 'base_SdssCentroid_xSigma', 'base_SdssCentroid_ySigma', 'base_SdssCentroid_flag',
'base_PsfFlux_area', 'base_ClassificationExtendedness_value', 'base_Blendedness_abs_flux',
'base_PixelFlags_flag_edge', 'base_PixelFlags_flag_interpolatedCenter', 'base_PixelFlags_flag_saturatedCenter',
'base_PixelFlags_flag_crCenter', 'base_PixelFlags_flag_bad', 'base_PixelFlags_flag_suspectCenter',
'base_PixelFlags_flag_clipped', 'deblend_skipped', 'base_PixelFlags_flag_edge',
'base_PixelFlags_flag_interpolatedCenter', 'base_PixelFlags_flag_saturatedCenter', 'base_PixelFlags_flag_crCenter',
'base_PixelFlags_flag_bad', 'base_PixelFlags_flag_suspectCenter', 'base_PixelFlags_flag_clipped',
'ext_shapeHSM_HsmSourceMoments_flag', 'ext_shapeHSM_HsmSourceMoments_xx', 'base_SdssShape_psf_xx',
'ext_shapeHSM_HsmSourceMoments_yy', 'base_SdssShape_psf_yy', 'ext_shapeHSM_HsmSourceMoments_xy',
'base_SdssShape_psf_xy', 'y_mag', 'y_mag_err', 'y_base_PsfFlux_flux', 'y_base_PsfFlux_flag',
'y_base_PsfFlux_fluxSigma', 'y_base_SdssShape_flag', 'y_base_SdssShape_xx', 'y_base_SdssShape_psf_xx',
'y_base_SdssShape_yy', 'y_base_SdssShape_psf_yy', 'y_base_SdssShape_xy', 'y_base_SdssShape_psf_xy',
'y_modelfit_CModel_flux', 'y_modelfit_CModel_flux', 'y_modelfit_CModel_fluxSigma', 'y_modelfit_CModel_flux',
'y_modelfit_CModel_fluxSigma', 'y_base_SdssShape_psf_xx', 'y_base_SdssShape_psf_yy', 'y_base_SdssShape_psf_xy', 'g_mag',
'g_mag_err', 'g_base_PsfFlux_flux', 'g_base_PsfFlux_flag', 'g_base_PsfFlux_fluxSigma', 'g_base_SdssShape_flag',
'g_base_SdssShape_xx', 'g_base_SdssShape_psf_xx', 'g_base_SdssShape_yy', 'g_base_SdssShape_psf_yy',
'g_base_SdssShape_xy', 'g_base_SdssShape_psf_xy', 'g_modelfit_CModel_flux', 'g_modelfit_CModel_flux',
'g_modelfit_CModel_fluxSigma', 'g_modelfit_CModel_flux', 'g_modelfit_CModel_fluxSigma', 'g_base_SdssShape_psf_xx',
'g_base_SdssShape_psf_yy', 'g_base_SdssShape_psf_xy', 'r_mag', 'r_mag_err', 'r_base_PsfFlux_flux',
'r_base_PsfFlux_flag', 'r_base_PsfFlux_fluxSigma', 'r_base_SdssShape_flag', 'r_base_SdssShape_xx',
'r_base_SdssShape_psf_xx', 'r_base_SdssShape_yy', 'r_base_SdssShape_psf_yy', 'r_base_SdssShape_xy',
'r_base_SdssShape_psf_xy', 'r_modelfit_CModel_flux', 'r_modelfit_CModel_flux', 'r_modelfit_CModel_fluxSigma',
'r_modelfit_CModel_flux', 'r_modelfit_CModel_fluxSigma', 'r_base_SdssShape_psf_xx', 'r_base_SdssShape_psf_yy',
'r_base_SdssShape_psf_xy', 'z_mag', 'z_mag_err', 'z_base_PsfFlux_flux', 'z_base_PsfFlux_flag',
'z_base_PsfFlux_fluxSigma', 'z_base_SdssShape_flag', 'z_base_SdssShape_xx', 'z_base_SdssShape_psf_xx',
'z_base_SdssShape_yy', 'z_base_SdssShape_psf_yy', 'z_base_SdssShape_xy', 'z_base_SdssShape_psf_xy',
'z_modelfit_CModel_flux', 'z_modelfit_CModel_flux', 'z_modelfit_CModel_fluxSigma', 'z_modelfit_CModel_flux',
'z_modelfit_CModel_fluxSigma', 'z_base_SdssShape_psf_xx', 'z_base_SdssShape_psf_yy', 'z_base_SdssShape_psf_xy', 'i_mag',
'i_mag_err', 'i_base_PsfFlux_flux', 'i_base_PsfFlux_flag', 'i_base_PsfFlux_fluxSigma', 'i_base_SdssShape_flag',
'i_base_SdssShape_xx', 'i_base_SdssShape_psf_xx', 'i_base_SdssShape_yy', 'i_base_SdssShape_psf_yy',
'i_base_SdssShape_xy', 'i_base_SdssShape_psf_xy', 'i_modelfit_CModel_flux', 'i_modelfit_CModel_flux',
'i_modelfit_CModel_fluxSigma', 'i_modelfit_CModel_flux', 'i_modelfit_CModel_fluxSigma', 'i_base_SdssShape_psf_xx',
'i_base_SdssShape_psf_yy', 'i_base_SdssShape_psf_xy', 'u_mag', 'u_mag_err', 'u_base_PsfFlux_flux',
'u_base_PsfFlux_flag', 'u_base_PsfFlux_fluxSigma', 'u_base_SdssShape_flag', 'u_base_SdssShape_xx',
'u_base_SdssShape_psf_xx', 'u_base_SdssShape_yy', 'u_base_SdssShape_psf_yy', 'u_base_SdssShape_xy',
'u_base_SdssShape_psf_xy', 'u_modelfit_CModel_flux', 'u_modelfit_CModel_flux', 'u_modelfit_CModel_fluxSigma',
'u_modelfit_CModel_flux', 'u_modelfit_CModel_fluxSigma', 'u_base_SdssShape_psf_xx', 'u_base_SdssShape_psf_yy',
'u_base_SdssShape_psf_xy']

 ### Generated with the following function
 ### But we don't actually want to regenerate this each time we run this script

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

    return columns_to_keep


def load_into_pandas(datafile, key_prefix='coadd'):
    import re
    r = re.search('merged_tract_([0-9]+)\.', datafile)
    tract = int(r[1])

    # Note '%d%d' instead of '%d,%d'
    nx, ny = 8, 8
    patches = ['%d%d' % (i, j) for i in range(nx) for j in range(ny)]

    dfs = []
    for patch in patches:
        key = '%s_%d_%s' % (key_prefix, tract, patch)
        try:
            df = pd.read_hdf(datafile, key=key)
        except:
            continue
        dfs.append(df)

    df = pd.concat(dfs)
    return df


def make_trim_file(infile, outfile):
    data = load_into_pandas(infile)

    trim_data = data[columns_to_keep]
    trim_data.write(outfile)


if __name__ == "__main__":
    import sys
    infile, outfile = sys.argv[1:3]
    make_trim_file(infile, outfile)
