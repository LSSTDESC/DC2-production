import os
import re
import numpy as np
import tables
import pandas as pd
from GCR import BaseGenericCatalog

__all__ = ['DC2StaticCoaddCatalog']

class DC2StaticCoaddCatalog(BaseGenericCatalog):

    _native_filter_quantities = {'tract', 'patch'}

    def _subclass_init(self, base_dir=None, use_cache=True, **kwargs):
        self.base_dir = base_dir or '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/summary/'
        assert os.path.isdir(self.base_dir), '{} is not a valid directory'.format(self.base_dir)
        self._datasets, self._columns = self._generate_native_datasets_and_columns(self.base_dir)
        assert self._datasets, 'No catalogs were found in {}'.format(self.base_dir)

        self._dataset_cache = dict()
        self._use_cache = True
        self.use_cache = use_cache

        self._quantity_modifiers = {
            'ra': 'coord_ra',
            'dec': 'coord_dec',
        }

        for band in 'ugrizY':
            self._quantity_modifiers['mag_{}_lsst'.format(band)] = '{}_mag'.format(band.lower())
            self._quantity_modifiers['magerr_{}_lsst'.format(band)] = '{}_mag_err'.format(band.lower())

    @staticmethod
    def _generate_native_datasets_and_columns(base_dir, filename_re=r'merged_tract_\d+\.hdf5', generate_column_list=True):
        datasets = list()
        columns = set()
        for fname in sorted((f for f in os.listdir(base_dir) if re.match(filename_re, f))):
            if fname == 'merged_tract_4849.hdf5': # this file does not have coadd_<tract>_<patch> keys
                continue
            fpath = os.path.join(base_dir, fname)
            with tables.open_file(fpath, 'r') as fh:
                datasets.extend(((fpath, key) for key in fh.root._v_children))
                columns.update((c.decode() for key in fh.root._v_children for c in fh.root[key].axis0))
        return datasets, columns

    @staticmethod
    def get_dataset_info(dataset):
        items = dataset[1].split('_')
        return dict(tract=int(items[1]), patch=int(items[2]))

    @property
    def use_cache(self):
        return self._use_cache

    @use_cache.setter
    def use_cache(self, value):
        self._use_cache = bool(value)
        if not self._use_cache:
            self._dataset_cache = dict()

    @property
    def available_tract_patches(self):
        return [self.get_dataset_info(dataset) for dataset in self._datasets]

    def load_dataset(self, dataset):
        if dataset in self._dataset_cache:
            d = self._dataset_cache[dataset]
        else:
            d = pd.read_hdf(*dataset, mode='r')
            if self.use_cache:
                self._dataset_cache[dataset] = d
        return d

    def _generate_native_quantity_list(self):
        return self._columns.union(self._native_filter_quantities)

    def _iter_native_dataset(self, native_filters=None):
        for dataset in self._datasets:
            dataset_info = self.get_dataset_info(dataset)
            if native_filters and \
                    not all(native_filter[0](*(dataset_info[c] for c in native_filter[1:])) \
                    for native_filter in native_filters):
                continue
            def native_quantity_getter(native_quantity):
                d = self.load_dataset(dataset)
                if native_quantity in self._native_filter_quantities:
                    return np.repeat(dataset_info[native_quantity], len(d))
                elif native_quantity not in d:
                    return np.repeat(np.nan, len(d))
                else:
                    return d[native_quantity].values
            yield native_quantity_getter
