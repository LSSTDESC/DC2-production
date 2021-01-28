---
name: DC2 Data Release Checklist
about: DC2 Data Release Checklist
title: DC2 Data Release [Insert Release ID here]
labels: Data products
assignees: ''

---

** Description **
Add a description of this data release, including its identifier, i.e. Run2.2i-dr6-v2

### Prepare Pre-Release Data

#### Assemble Functional LSST Science Pipeline repository and determine the appropriate name for this release i.e. Run2.2i-dr6-v2
- [ ] calibrations, including Brighter-Fatter
- [ ] reference catalog
- [ ] ingested raws
- [ ] calexps
- [ ] coadds
- [ ] multiband

#### Catalogs

- [ ] Copy Object Catalogs into the shared area to a directory named: Run*-dr*-v*pre/object_table_summary
/global/cfs/cdirs/lsst/shared/DC2-prod/*/dpdd
- [ ] Copy Metacal, if available, into the shared area in a directory named: Run*-dr*-v*pre/metacal_table_summary
/global/cfs/cdirs/lsst/shared/DC2-prod/*/dpdd
- [ ] Produce Truth Match (is there a write up for how this is done?)
- [ ] Copy output into shared area: /global/cfs/cdirs/lsst/shared/DC2-prod/*/truth
- [ ] Update GCRCatalogs for pre-release including the object catalogs and metacal
- [ ] Produce [DPDD Parquet using Object Catalogs](https://github.com/LSSTDESC/DC2-production/blob/master/scripts/README_write_gcr_to_parquet.md)
Store output in the shared area: /global/cfs/cdirs/lsst/shared/DC2-prod/*/dpdd/Run*-dr*-v*pre

#### Add-On Catalogs

- [ ] Inform PZ group and request Photo-Z
When available, copy into shared /global/cfs/cdirs/lsst/shared/DC2-prod/*/addons/photoz
- [ ] Request generation of survey property maps using supreme
When available, copy into shared /global/cfs/cdirs/lsst/shared/DC2-prod/*/addons/supreme

#### Validation

- [ ] Run [validation notebook](https://github.com/LSSTDESC/DC2-analysis/tree/master/validation)
- [ ] Request DC2 team to take a look

#### Final Preparations

- [ ] Rename /global/cfs/cdirs/lsst/shared/DC2-prod/*/dpdd/Run*-dr*-v*pre to Run*-dr*-v*, effectively removing the "pre" suffix, i.e. Run2.2i-dr6-v2
- [ ] Update or create symlink pointing to the new Run*-dr*-v* directory, naming it Run*-dr*, i.e. Run2.2i-dr6 is a symlink pointing to Run2.2i-dr6-v2
- [ ] Update and release GCRCatalogs including all available add-on catalogs, object cats, truth match, etc
- [ ] Copy LSST Science Pipelines repository to shared area
- [ ] Update [Data Products Page](https://confluence.slac.stanford.edu/display/LSSTDESC/DC2+Data+Product+Overview)
- [ ] Update and tag desc-dc2-dm-data
- [ ] Post announcement on Slack
