from merge_tract_cat import example_load_patch

tract=4849
patch='1,1'

patch_cat = example_load_patch(tract=tract, patch=patch)

filebase = 'merged_tract_%d_%s' % (tract, patch)
patch_cat.write(filebase+'.ecsv', format='ascii.ecsv')

# Each of the following is expected to fail:
### Column names too long for FITS file standard
patch_cat.write(filebase+'.fits', format='fits')
### No PyTables available on Cori:
patch_cat.to_pandas().to_hdf(filebase+'.h5', 'table')
