DC2 Notebooks
=============

This directory contains tutorial and demonstration notebooks convering how to access and use the DC2 datasets.
See the index table below for links to the notebook code, and an auto-rendered view of the notebook with outputs.
Notes on how to contribute more notebooks, and how the rendering is made, are at the bottom of the page.

.. list-table::
   :widths: 10 20 10 10
   :header-rows: 1

   * - Notebook
     - Short description
     - Links
     - Owner

   * - DC2 Reference Catalog Reader
     - Simple example of reading the reference catalog
     - `ipynb <DC2%20Reference%20Catalog%20Reader.ipynb>`_,
       `rendered <https://nbviewer.jupyter.org/github/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/DC2_Reference_Catalog_Reader.nbconvert.ipynb>`_
       
       .. image:: https://github.com/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/log/DC2_Reference_Catalog_Reader.svg
          :target: https://github.com/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/log/DC2_Reference_Catalog_Reader.log
      
      - `Anze Slosar <https://github.com/LSSTDESC/DC2_Repo/issues/new?body=@slosar>`_

   * - DC2 Coadd Run1.1p GCR access HSC selection
     - Use the GCR to apply the HSC object selection cuts
     - `ipynb <DC2%20Coadd%20Run1.1p%20GCR%20access%20--%20HSC%20selection.ipynb>`_,
       `rendered <https://nbviewer.jupyter.org/github/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/DC2_Coadd_Run1.1p_GCR_access_--_HSC_selection.nbconvert.ipynb>`_
       
       .. image:: https://github.com/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/log/DC2_Coadd_Run1.1p_GCR_access_--_HSC_selection.svg
          :target: https://github.com/LSSTDESC/DC2_Repo/blob/rendered/Notebooks/log/DC2_Coadd_Run1.1p_GCR_access_--_HSC_selection.log
       
     - `Yao-Yuan Mao <https://github.com/LSSTDESC/DC2_Repo/issues/new?body=@yymao>`_

----

Notes for Contributors
----------------------
Both tutorial and demo notebooks are hugely useful resources - pull requests are most welcome!
* Before you commit a notebook, please make sure that a) it runs to completion and b) the outputs are cleared (to avoid repo bloat).
* Please do update the index table above, carefully copying and adapting the URLs.
* The "owner" of a notebook (that's you, as contributor!) is responsible for accepting proposed modifications to it (by collaboration), and making sure that it does not go stale (by fixing issues posted about it).
* Every tutorial notebook needs an owner/last verified header, a statement of its goals (learning objectives) in the first markdown cell, and enough explanatory markdown (with links to docs, papers etc) to make the notebook make sense.

Continuous Integration
----------------------
All the notebooks in the folder can be run on Cori using the [`beavis-ci` script](beavis-ci.csh), which then pushes them to an orphan "rendered" branch so that the outputs can be viewed. At present, DC2_Repo admin permissions are needed to execute this push.

    If the link to a rendered notebook yields a 404 error, please check the corresponding log file and issue the notebook's owner.
