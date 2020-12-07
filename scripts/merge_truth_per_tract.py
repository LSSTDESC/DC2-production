#!/usr/bin/env python

"""
Merge truth catalogs for a given tract and match to object catalog
Author: Yao-Yuan Mao
"""
import os
import re
import warnings
from argparse import ArgumentParser, RawTextHelpFormatter

import numpy as np
from astropy.coordinates import SkyCoord
import pandas as pd

__all__ = ["merge_truth_per_tract", "match_object_with_merged_truth"]


def merge_truth_per_tract(input_dir, truth_types=("gal", "star", "sn"), validate=False, silent=False, **kwargs):
    """Merge all the truth catalogs in one single tract directory.

    This function assumes the truth catalogs are in parquet format and have specifc file name patterns.
    The filename prefix should match one of those specified in truth_types.
    For galaxy type, it is further assumed that the cosmodc2 healpix id is embedded in the filename.

    Parameters
    ----------
    input_dir : str
        Path to the input tract directory

    Optional Parameters
    ----------------
    truth_types : tuple of str, optional (default: ("gal", "star", "sn"))
        The types of truth objects. The string should be the prefix of truth files.
    validate : bool, optional (default: False)
        If true, check the tract column has only one value
    silent : bool, optional (default: False)
        If true, turn off most printout.
    """
    my_print = (lambda x: None) if silent else print

    my_print("Merging all files in", input_dir)
    files_to_merge = sorted(os.listdir(input_dir))
    df_to_merge = []
    for filename in files_to_merge:

        my_print("Loading", filename)

        # determine truth type
        type_code = 0
        for i, type_prefix in enumerate(truth_types):
            if filename.startswith(type_prefix):
                type_code = i + 1

        # obtain healpix id for galaxies (type_code == 1)
        healpix = -1
        if type_code == 1:
            m = re.search(r"_hp(\d+)\b", filename)
            if m is None:
                warnings.warn("Cannot identify healpix id in", filename)
            else:
                healpix = int(m.groups()[0])

        # read in files and add columns
        df = pd.read_parquet(os.path.join(input_dir, filename))
        df["truth_type"] = type_code
        df["cosmodc2_hp"] = healpix
        df["cosmodc2_id"] = df["id"] if type_code == 1 else -1
        df["id"] = df["id"].astype(str)
        # TODO: this is added to maintain compatibility with validateion notebook. Remove this when finalize schema.
        df["is_sn"] = (type_code == 3)

        df_to_merge.append(df)

    df = pd.concat(df_to_merge, ignore_index=True)
    del df_to_merge

    if validate:
        assert len(np.unique(df["tract"].values)) == 1

    return df


def match_object_with_merged_truth(truth_cat, object_cat, validate=False, silent=False, **kwargs):
    """Match object catalog with truth catalog

    Parameters
    ----------
    truth_cat : pd.DataFrame or str
        Truth catalog or its path (needs to be a parquet file)
    object_cat : pd.DataFrame or str
        Object catalog or its path (needs to be a parquet file)

    Optional Parameters
    ----------------
    validate : bool, optional (default: False)
        If true, check the matching is perform properly.
    silent : bool, optional (default: False)
        If true, turn off most printout.
    """
    my_print = (lambda x: None) if silent else print

    if isinstance(truth_cat, str):
        my_print("Loading truth catalog from", truth_cat)
        truth_cat = pd.read_parquet(truth_cat)

    if isinstance(object_cat, str):
        my_print("Loading object catalog from", object_cat)
        object_cat = pd.read_parquet(object_cat, columns=["objectId", "ra", "dec"])

    my_print("Performing nearest neighbor match")
    object_sc = SkyCoord(object_cat["ra"].values, object_cat["dec"].values, unit="deg")
    truth_sc = SkyCoord(truth_cat["ra"].values, truth_cat["dec"].values, unit="deg")
    idx, sep, _ = object_sc.match_to_catalog_sky(truth_sc)
    del object_sc, truth_sc

    my_print("Merging matching info with truth catalog")
    matched = pd.DataFrame.from_dict({
        "truth_idx": truth_cat.index.values[idx],
        "match_sep": sep.arcsec,
        "match_objectId": object_cat["objectId"]
    })
    del idx, sep

    matched.sort_values("match_sep", inplace=True)
    matched["is_unique_truth_entry"] = ~matched.duplicated("truth_idx", keep="first")
    matched.sort_index(inplace=True)

    not_matched_truth_idx = truth_cat.index.values[np.in1d(truth_cat.index.values, matched["truth_idx"].values, invert=True)]
    not_matched = pd.DataFrame.from_dict({
        "truth_idx": not_matched_truth_idx,
        "match_sep": -1.0,
        "match_objectId": -1,
        "is_unique_truth_entry": True
    })

    full_table = pd.concat([matched, not_matched], ignore_index=True)
    del matched, not_matched, not_matched_truth_idx

    full_table = pd.merge(truth_cat, full_table, left_index=True, right_on="truth_idx", how="outer")
    full_table.sort_index(inplace=True)

    if validate:
        objid = full_table["match_objectId"].values
        assert (objid[objid > -1] == object_cat["objectId"].values).all()
        del objid
        truth_idx = full_table["truth_idx"].values[full_table["is_unique_truth_entry"].values]
        truth_idx.sort()
        assert (truth_idx == truth_cat.index.values).all()
        del truth_idx

    del full_table["truth_idx"]

    return full_table


def save_df_to_disk(df, output_dir, name="truth", silent=False, **kwargs):
    my_print = (lambda x: None) if silent else print

    tract = df.loc[0, "tract"]
    output_path = os.path.join(output_dir, "{}_tract{}.parquet".format(name, tract))

    my_print("Writing output to disk at", output_path)
    df.to_parquet(output_path, index=False, engine="pyarrow", flavor="spark")


def main():
    usage = """Merge truth catalogs for a given tract, and then, optionally, match to object catalog

To merge a repartitioned truth catalog, run
  python %(prog)s /path/to/repartitioned/truth/<tract>

Usually you may want to match with the object catalog too:
  python %(prog)s /path/to/repartitioned/truth/<tract> --object=/path/to/object-catalog_tract<tract>.parquet

If you have a ready-to-use truth catalog, you can skip the merge step, and only run the matching code:
  python %(prog)s /path/to/truth.parquet --object=/path/to/object-catalog_tract<tract>.parquet --matching-only

"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument("input_dir", help="Input directory that corresponds to one tract")
    parser.add_argument('--name', default='truth',
                        help='Base name of files: <name>_tract5062.parquet (default: %(default)s)')
    parser.add_argument('-o', '--output-dir', default='.', help='Output directory.  (default: %(default)s)')
    parser.add_argument("--object-catalog-path", help="Path to the object catalog for sky match")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--silent", action="store_true")
    parser.add_argument("--matching-only", action="store_true")

    args = parser.parse_args()

    if not args.matching_only:
        df = merge_truth_per_tract(**vars(args))

    if args.object_catalog_path:
        if args.matching_only:
            df = args.input_dir
        df = match_object_with_merged_truth(df, args.object_catalog_path, **vars(args))
    elif args.matching_only:
        parser.error("--object-catalog-path must be specified when --matching-only is set.")

    save_df_to_disk(df, **vars(args))


if __name__ == "__main__":
    main()
