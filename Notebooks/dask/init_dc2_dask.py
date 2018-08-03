# source /global/common/software/lsst/common/miniconda/setup_current_python.sh

from dask_jobqueue import SLURMCluster
from dask.distributed import Client

cluster = SLURMCluster()
cluster.scale(2)

client = Client(cluster)
