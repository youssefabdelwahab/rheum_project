import torch.linalg 
import torch
import torch.nn as nn
import math

def compute_eigenvectors(edge_index: torch.Tensor, num_nodes: int , k_freq: int = 8) -> torch.Tensor:

    """
    Build static geometric map of entire document; graph l;
    
    """

    A = torch.zeros((num_nodes, num_nodes), dtype=torch.float32)
    A[edge_index[0], edge_index[1]] = 1.0 
    A = torch.max(A, A.T)

    row_degrees = A.sum(dim=1)
    deg_inv_sqrt = row_degrees.pow(-0.5)
    A_norm = deg_inv_sqrt.unsqueeze(1) * A * deg_inv_sqrt.unsqueeze(0)

    I = torch.eye(num_nodes, dtype=torch.float32)
    L = I - A_norm  

    eigenvalues, eigenvectors = torch.linalg.eigh(L)
    topological_coordinates = eigenvectors[:, 1 : k_freq + 1]

    return topological_coordinates



class WireEngine(nn.Module): 
    def __init__(self, k_freq:int , d_attention: int): 
        """
        Args:
            k_freq (int): The number of Laplacian eigenvectors (e.g., 16).
            d_attention (int): The dimension of the query/key vectors in the attention head.
        """

        super().__init__()
        self.k_freq = k_freq
        self.d_attention = d_attention

        self.d_half = d_attention // 2
        self.omega_matrix = nn.Linear(k_freq, self.d_half, bias=False)
        self._init_exponential_frequencies()


    def _init_exponential_frequencies(self):
        """
        Applies the exponential decay initialization to mirror NLP RoPE behavior,
        allowing lower dimensions to rotate quickly (local topology) and higher 
        dimensions to rotate slowly (global topology).
        """
        with torch.no_grad():
            # Generate uniform random frequencies
            rand_freqs = torch.rand(self.d_half, self.k_freq)
            
            # Apply the 10,000 exponential decay factor
            decay_factors = torch.tensor(
                [[10000**(2 * i / self.d_half) for _ in range(self.k_freq)] 
                 for i in range(self.d_half)]
            )
            
            # Copy into the linear layer weights
            self.omega_matrix.weight.copy_(rand_freqs / decay_factors)

    def rotate(self, x: torch.Tensor, sin: torch.Tensor, cos: torch.Tensor) -> torch.Tensor:
        """
        Vectorized 2D rotation for standard float tensors.
        Args:
            x: Tensor of shape [..., d_attention]
            sin, cos: Tensors of shape [..., d_half]
        """
        # Slice into evens and odds
        x1, x2 = x[..., ::2], x[..., 1::2]
        
        # Apply 2D rotation matrix math
        x_rotated = torch.stack([x1 * cos - x2 * sin, x1 * sin + x2 * cos], dim=-1)
        
        # Flatten the last two dimensions to restore the original shape [..., d_attention]
        return x_rotated.flatten(-2)