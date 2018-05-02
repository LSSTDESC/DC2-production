import os
import re
import warnings
import numpy as np
import tables
import pandas as pd
from GCR import BaseGenericCatalog

__all__ = ['DC2StaticCoaddCatalog']

class DC2StaticCoaddCatalog(BaseGenericCatalog):

    _native_filter_quantities = {'tract', 'patch'}

    def _subclass_init(self, base_dir=None, **kwargs):
        self.base_dir = base_dir or '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/summary/'
        assert os.path.isdir(self.base_dir), '{} is not a valid directory'.format(self.base_dir)
        self._datasets, self._columns = self._generate_native_datasets_and_columns(self.base_dir)
        assert self._datasets, 'No catalogs were found in {}'.format(self.base_dir)
        self._dataset_cache = dict()

        self._quantity_modifiers = {
            'ra': 'coord_ra',
            'dec': 'coord_dec',
        }

        for band in 'ugrizY':
            self._quantity_modifiers['mag_{}_lsst'.format(band)] = '{}_mag'.format(band.lower())
            self._quantity_modifiers['magerr_{}_lsst'.format(band)] = '{}_mag_err'.format(band.lower())

    @staticmethod
    def _generate_native_datasets_and_columns(base_dir, filename_re=r'merged_tract_\d+\.hdf5', groupname_re=r'coadd_\d+_\d+$'):
        datasets = list()
        columns = set()
        for fname in sorted((f for f in os.listdir(base_dir) if re.match(filename_re, f))):
            fpath = os.path.join(base_dir, fname)
            datasets_this = list()
            columns_this = set()
            try:
                with tables.open_file(fpath, 'r') as fh:
                    for key in fh.root._v_children:
                        if not re.match(groupname_re, key):
                            warnings.warn('{} does not have correct group names; skipped'.format(fname))
                            break
                        if 'axis0' not in fh.root[key]:
                            warnings.warn('{} does not have correct hdf5 format; skipped'.format(fname))
                            break
                        datasets_this.append((fpath, key))
                        columns_this.update((c.decode() for c in fh.root[key].axis0))
                    else:
                        datasets.extend(datasets_this)
                        columns.update(columns_this)
            except (IOError, OSError):
                warnings.warn('Cannot access {}; skipped'.format(fpath))
        return datasets, columns

    @staticmethod
    def get_dataset_info(dataset):
        items = dataset[1].split('_')
        return dict(tract=int(items[1]), patch=int(items[2]))

    @property
    def available_tract_patches(self):
        return [self.get_dataset_info(dataset) for dataset in self._datasets]

    def clear_cache(self):
        self._dataset_cache = dict()

    def _load_dataset(self, dataset):
        return pd.read_hdf(*dataset, mode='r')

    def load_dataset(self, dataset):
        if dataset not in self._dataset_cache:
            try:
                self._dataset_cache[dataset] = self._load_dataset(dataset)
            except MemoryError:
                self.clear_cache()
                self._dataset_cache[dataset] = self._load_dataset(dataset)
        return self._dataset_cache[dataset]

    def _generate_native_quantity_list(self):
        return self._columns.union(self._native_filter_quantities)

    def _iter_native_dataset(self, native_filters=None):
        for dataset in self._datasets:
            dataset_info = self.get_dataset_info(dataset)
            if native_filters and \
                    not all(native_filter[0](*(dataset_info[c] for c in native_filter[1:])) \
                    for native_filter in native_filters):
                continue
            d = self.load_dataset(dataset)
            def native_quantity_getter(native_quantity):
                if native_quantity in self._native_filter_quantities:
                    return np.repeat(dataset_info[native_quantity], len(d))
                elif native_quantity not in d:
                    return np.repeat(np.nan, len(d))
                else:
                    return d[native_quantity].values
            yield native_quantity_getter

