from merge_tract_cat import example_load_patch

tract=4849
patch='1,1'

patch_cat = example_load_patch(tract=tract, patch=patch)

filebase = 'merged_tract_%d_%s' % (tract, patch)
patch_cat.write(filebase+'.ecsv', format='ascii.ecsv')
patch_cat.write(filebase+'.fits', format='fits')
# patch_cat.to_pandas().to_hdf(filebase+'.h5', 'table')
