#!/usr/bin/env python

"""
Merge truth catalogs for a given tract and match to object catalog
Author: Yao-Yuan Mao
"""
import os
import re
from argparse import ArgumentParser, RawTextHelpFormatter

import numpy as np
from astropy.coordinates import SkyCoord
import pandas as pd

__all__ = ["merge_truth_per_tract"]


def merge_truth_per_tract(
    input_dir,
    name,
    output_dir,
    object_catalog_path=None,
    validate=False,
    silent=False,
    **kwargs
):
    """
    TODO: add docstring
    """
    my_print = (lambda x: None) if silent else print

    files_to_merge = sorted(os.listdir(input_dir))
    df_to_merge = []
    for filename in files_to_merge:
        df = pd.read_parquet(os.path.join(input_dir, filename))
        m = re.search(r"_hp(\d+)\b", filename)
        if m is not None:
            hp = int(m.groups()[0])
            df["cosmodc2_hp"] = hp
            df["cosmodc2_id"] = df["id"]
            df["id"] = df["id"].astype(str)
        else:
            df["cosmodc2_hp"] = -1
            df["cosmodc2_id"] = -1
        df["is_sn"] = True if filename.startswith("sn_") else False
        df_to_merge.append(df)
    df = pd.concat(df_to_merge, ignore_index=True)
    del df_to_merge

    tract = df.loc[0, "tract"]
    output_path = os.path.join(output_dir, "{}_tract{}.parquet".format(name, tract))

    if not object_catalog_path:
        my_print("No object catalog given! No matching is done!")
        df.to_parquet(output_path, index=False)
        return

    object_cat = pd.read_parquet(object_catalog_path, columns=["objectId", "ra", "dec"])

    object_sc = SkyCoord(object_cat["ra"].values, object_cat["dec"].values, unit="deg")
    truth_sc = SkyCoord(df["ra"].values, df["dec"].values, unit="deg")
    idx, sep, _ = object_sc.match_to_catalog_sky(truth_sc)
    del object_sc, truth_sc

    matched = pd.DataFrame.from_dict({"idx": idx, "sep": sep.arcsec, "objectId": object_cat["objectId"]})
    del idx, sep

    matched.sort_values("sep", inplace=True)
    matched["is_unique_truth_entry"] = ~matched.duplicated("idx", keep="first")
    matched.sort_index(inplace=True)

    not_matched_truth_idx = np.flatnonzero(np.in1d(df.index.values, matched["idx"].values, invert=True))
    not_matched = pd.DataFrame.from_dict({"idx": not_matched_truth_idx, "sep": -1.0, "objectId": -1, "is_unique_truth_entry": True})

    full_table = pd.concat([matched, not_matched], ignore_index=True)
    del matched, not_matched, not_matched_truth_idx

    full_table = pd.merge(full_table, df, left_on="idx", right_index=True, how="outer", validate="many_to_one")
    full_table.sort_index(inplace=True)

    if validate:
        objid = full_table["objectId"].values
        assert (objid[objid > -1] == object_cat["objectId"].values).all()
        del objid
        truth_idx = full_table["idx"].values[full_table["is_unique_truth_entry"].values]
        truth_idx.sort()
        assert (truth_idx == df.index.values).all()
        del truth_idx

    del full_table["idx"], df, object_cat

    full_table.to_parquet(output_path, index=False)


def main():
    usage = """TODO: add usage
"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument("input_dir", help="Input directory that corresponds to one tract")
    parser.add_argument('--name', default='truth',
                        help='Base name of files: <name>_tract_5062.parquet')
    parser.add_argument('-o', '--output-dir', default='.',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument("--object-catalog-path")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--silent", action="store_true")

    merge_truth_per_tract(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
