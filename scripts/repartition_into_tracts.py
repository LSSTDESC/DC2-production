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
    **kwargs
):
    """
    TODO: add docstring
    """
    # obtain skymap
    repo = desc_dc2_dm_data.REPOS.get(skymap_source_repo, skymap_source_repo)
    skymap = Butler(repo).get("deepCoadd_skyMap")

    # load input parquet file
    df = pd.read_parquet(input_file)

    # find tract and patch for each row
    tract, patch = [], []
    for ra, dec in tqdm(zip(df[ra_label], df[dec_label]), total=len(df)):
        tract_this, patch_this = get_tract_patch(skymap, ra, dec)
        tract.append(tract_this)
        patch.append(patch_this)
    df["tract"] = tract
    df["patch"] = patch
    del tract, patch

    # write out each tract
    for tract, df_this_tract in df.groupby("tract"):
        output_dir = os.path.join(output_root_dir, str(tract))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, input_file)
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

    repartition_into_tracts(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
