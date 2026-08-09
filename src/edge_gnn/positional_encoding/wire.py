import torch
import torch.nn as nn
import math
from torch_geometric.utils import to_undirected, get_laplacian, to_dense_adj

def compute_eigenvectors(edge_index: torch.Tensor, num_nodes: int, k_freq: int = 8) -> torch.Tensor:
    """
    Computes the Laplacian eigenvectors to build a static geometric map of the document.

    This function calculates the symmetric normalized graph Laplacian and derives 
    its eigenvectors. The resulting vectors serve as global positional encodings, 
    capturing the physical topology of the text document. It explicitly drops the 
    first trivial eigenvector (eigenvalue 0) and returns the next `k_freq` vectors.

    Args:
        edge_index (torch.Tensor): A sparse graph connectivity tensor of shape [2, E].
        num_nodes (int): The total number of nodes in the graph.
        k_freq (int, optional): The number of lowest-frequency eigenvectors to retain 
            for the spatial coordinates. Defaults to 8.

    Returns:
        torch.Tensor: The topological coordinates for each node, yielding a tensor 
            of shape [num_nodes, k_freq].
    """
  
    edge_index = to_undirected(edge_index, num_nodes=num_nodes)


    lap_edge_index, lap_edge_weight = get_laplacian(
        edge_index, 
        normalization='sym', 
        num_nodes=num_nodes
    )


    L = to_dense_adj(
        lap_edge_index, 
        edge_attr=lap_edge_weight, 
        max_num_nodes=num_nodes
    ).squeeze(0)

    eigenvalues, eigenvectors = torch.linalg.eigh(L)
    
    
    topological_coordinates = eigenvectors[:, 1 : k_freq + 1]

    return topological_coordinates


class WireEngine(nn.Module): 
    def __init__(self, k_freq:int , d_attention: int): 
        """
    Wave-Induced Rotary Embeddings (WIRE) engine for graph attention.

    This module translates the static structural coordinates (Laplacian eigenvectors) 
    into dynamic rotational frequencies. It adapts the exponential decay mechanism 
    from NLP Rotary Position Embeddings (RoPE) for graph topologies, allowing the 
    attention mechanism to geometrically rotate query and key vectors. Lower embedding 
    dimensions rotate quickly to capture local graph topology, while higher dimensions 
    rotate slowly to capture global structure.

    Args:
        k_freq (int): The number of Laplacian eigenvectors provided per node.
        d_attention (int): The feature dimension of the query and key vectors 
            within a single attention head. Must be an even number.

    Attributes:
        d_half (int): Half of the attention dimension, corresponding to the number 
            of 2D rotation planes.
        omega_matrix (torch.nn.Linear): A learnable linear layer without bias that 
            maps the `k_freq` structural coordinates to `d_half` target frequencies.
    """
        super().__init__()
        self.k_freq = k_freq
        self.d_attention = d_attention

        self.d_half = d_attention // 2
        self.omega_matrix = nn.Linear(k_freq, self.d_half, bias=False)
        self._init_exponential_frequencies()

    def _init_exponential_frequencies(self):
        """
        Initializes the frequency projection matrix with exponential decay.

        This custom initialization mirrors the RoPE methodology. It generates uniform 
        random frequencies and scales them down exponentially across the attention 
        dimensions (by a factor up to 10,000). This ensures that the structural rotations 
        span a wide spectrum of frequencies, structurally embedding both near and far nodes.
        """
        with torch.no_grad():
            # Guniform random frequencies
            rand_freqs = torch.rand(self.d_half, self.k_freq)
            
            # 10,000 exponential decay factor
            decay_factors = torch.tensor(
                [[10000**(2 * i / self.d_half) for _ in range(self.k_freq)] 
                 for i in range(self.d_half)]
            )
            
            # Copy into the linear layer weights
            self.omega_matrix.weight.copy_(rand_freqs / decay_factors)

    def rotate(self, x: torch.Tensor, sin: torch.Tensor, cos: torch.Tensor) -> torch.Tensor:
        """
        Applies a vectorized 2D rotary transformation to the input tensor.

        This function splits the input tensor's feature dimension into even and odd 
        halves (representing x and y coordinates in 2D planes), and rotates them 
        using the provided sine and cosine matrices.

        Args:
            x (torch.Tensor): The input query or key tensor of shape [..., d_attention].
            sin (torch.Tensor): The sine frequency matrix of shape [..., d_half].
            cos (torch.Tensor): The cosine frequency matrix of shape [..., d_half].

        Returns:
            torch.Tensor: The geometrically rotated query or key tensor, restored to 
                its original shape [..., d_attention].
        """
        #  evens and odds
        x1, x2 = x[..., ::2], x[..., 1::2]
        
        x_rotated = torch.stack([x1 * cos - x2 * sin, x1 * sin + x2 * cos], dim=-1)
        
        return x_rotated.flatten(-2)