#!/usr/bin/env python

"""
Repartition a parquet file into tracts
"""
import os
from argparse import ArgumentParser, RawTextHelpFormatter

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


def repartition_into_tracts(
    input_file,
    output_root_dir,
    skymap_source_repo,
    ra_label="ra",
    dec_label="dec",
    silent=False,
    **kwargs
):
    """
    TODO: add docstring
    """
    my_print = (lambda x: None) if silent else print
    tqdm_disable = silent or None

    repo = desc_dc2_dm_data.REPOS.get(skymap_source_repo, skymap_source_repo)
    my_print("Obtain skymap from", repo)
    skymap = Butler(repo).get("deepCoadd_skyMap")

    my_print("Loading input parquet file", input_file)
    df = pd.read_parquet(input_file)

    my_print("Finding tract and patch for each row")
    tract, patch = [], []
    for ra, dec in tqdm(zip(df[ra_label], df[dec_label]), total=len(df), disable=tqdm_disable):
        tract_this, patch_this = get_tract_patch(skymap, ra, dec)
        tract.append(tract_this)
        patch.append(patch_this)
    df["tract"] = tract
    df["patch"] = patch
    del tract, patch

    my_print("Writing out parquet file for each tract in", output_root_dir)
    for tract, df_this_tract in tqdm(df.groupby("tract"), total=df["tract"].nunique(False), disable=tqdm_disable):
        output_dir = os.path.join(output_root_dir, str(tract))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, os.path.basename(input_file))
        df_this_tract.to_parquet(output_path, index=False)


def main():
    usage = """
    TODO: add usage
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
