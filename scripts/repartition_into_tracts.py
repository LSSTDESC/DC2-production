#!/usr/bin/env python

"""
Repartition a parquet file into tracts
Author: Yao-Yuan Mao
"""
import os
import multiprocessing as mp
from argparse import ArgumentParser, RawTextHelpFormatter

import numpy as np
import pandas as pd
from tqdm import tqdm

import lsst.geom
from lsst.daf.persistence import Butler
import desc_dc2_dm_data


__all__ = ["get_tract_patch", "repartition_into_tracts"]


def get_tract_patch(skymap, ra, dec):
    radec = lsst.geom.SpherePoint(ra, dec, lsst.geom.degrees)
    tractInfo = skymap.findTract(radec)
    patchInfo = tractInfo.findPatch(radec)
    return tractInfo.getId(), "{},{}".format(*patchInfo.getIndex())


def get_tract_patch_arrays(skymap, ra_arr, dec_arr, disable_tqdm=None):
    tract, patch = [], []
    for ra, dec in tqdm(zip(ra_arr, dec_arr), total=len(ra_arr), disable=disable_tqdm):
        tract_this, patch_this = get_tract_patch(skymap, ra, dec)
        tract.append(tract_this)
        patch.append(patch_this)
    return tract, patch


def repartition_into_tracts(
    input_file,
    output_root_dir,
    skymap_source_repo,
    ra_label="ra",
    dec_label="dec",
    n_cores=None,
    silent=False,
    **kwargs
):
    """ Take a parquet catalog and split it into tracts according to a given skymap, and write to disk

    Parameters
    ----------
    input_file : str
        Path to the input parquet file.
    output_root_dir : str
        Path to the output directory. The output files will have the following filename
        <output_root_dir>/<tract>/<input_file_basename>
    skymap_source_repo : str
        Path or existing key if desc_dc2_dm_data.REPOS to indicate the bulter repo for loading skymap

    Optional Parameters
    ----------------
    ra_label : str, optional
        Column name for RA, default to 'ra'. The unit is assumed to be degrees.
    dec_label : str, optional
        Column name for Dec, default to 'dec'. The unit is assumed to be degrees.
        If None (default), store all columns (see also `include_native`)
    silent : bool, optional (default: False)
        If true, turn off most printout.
    """
    my_print = (lambda x: None) if silent else print
    tqdm_disable = silent or None

    repo = desc_dc2_dm_data.REPOS.get(skymap_source_repo, skymap_source_repo)
    my_print("Obtain skymap from", repo)
    skymap = Butler(repo).get("deepCoadd_skyMap")

    my_print("Loading input parquet file", input_file)
    df = pd.read_parquet(input_file)

    n_cores = n_cores or os.cpu_count() or 1
    my_print("Finding tract and patch for each row, using", n_cores, "cores")
    ra_split = np.array_split(df[ra_label].values, n_cores)
    dec_split = np.array_split(df[dec_label].values, n_cores)
    tqdm_split = [True] * n_cores
    tqdm_split[0] = tqdm_disable
    with mp.Pool(n_cores) as pool:
        tractpatch = pool.map(lambda args: get_tract_patch_arrays(skymap, *args), zip(ra_split, dec_split, tqdm_split))
    df["tract"] = np.concatenate([tp[0] for tp in tractpatch])
    df["patch"] = np.concatenate([tp[1] for tp in tractpatch])
    del tractpatch

    my_print("Writing out parquet file for each tract in", output_root_dir)
    for tract, df_this_tract in tqdm(df.groupby("tract"), total=df["tract"].nunique(False), disable=tqdm_disable):
        output_dir = os.path.join(output_root_dir, str(tract))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, os.path.basename(input_file))
        df_this_tract.to_parquet(output_path, index=False)


def main():
    usage = """Take a parquet catalog and split it into tracts according to a given skymap, and write to disk

For example, to repartition a truth catalog, you can run

  python %(prog)s truth_summary_hp10068.parquet -o $CSCRATCH/truth_repartition

The output files will be put into directories:
   $CSCRATCH/truth_repartition/3259/truth_summary_hp10068.parquet
   $CSCRATCH/truth_repartition/3260/truth_summary_hp10068.parquet
   ...
With each tract directory can be then merged to produce a single file.
"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument("input_file", help="Parquet file to read.")
    parser.add_argument("-o", '--output-root-dir', default='.', help="Output root directory.")
    parser.add_argument("--skymap-source-repo", default="2.2i_dr6_wfd")
    parser.add_argument("--silent", action="store_true")

    repartition_into_tracts(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
