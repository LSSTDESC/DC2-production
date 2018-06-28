Catalog readers have been moved to [GCRCatalogs](https://github.com/LSSTDESC/gcr-catalogs).

Use, for example, this code to load the DC2 Coadd Catalog:

```python
import GCRCatalogs
catalog = GCRCatalogs.load_catalog('dc2_coadd_run1.1')
```