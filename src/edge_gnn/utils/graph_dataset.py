import torch
import os
import pandas as pd # Assuming raw data is in CSV/JSON
from typing import Callable, Optional
from torch_geometric.data import InMemoryDataset, Data
from torch_geometric.transforms import AddLaplacianEigenvectorPE

