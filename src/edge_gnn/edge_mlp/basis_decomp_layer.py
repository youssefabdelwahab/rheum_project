import torch
import torch.nn as nn
import torch.nn.functional as F




class Basis_Layer(nn.Module): 
    """
    Constructs edge-specific transformation matrices from a learned dictionary of basis matrices.

    This layer maintains a learnable bank of fundamental transformation matrices (the "basis"). 
    During the forward pass, it takes the edge-specific relational coefficients (generated 
    by the HolE MLP) and uses them to compute a weighted linear combination of the basis 
    matrices. This generates a unique, tailored transformation matrix for every single edge 
    in the graph.

    Args:
        d_length (int): The dimensionality of the square transformation matrices (typically 
            matching the hidden dimension of the node features).
        B (int): The number of basis matrices in the learnable bank. Currently constrained 
            to equal `d_length`.

    Attributes:
        basis_vector_bank (torch.nn.Parameter): A 3D tensor of shape [B, d_length, d_length] 
            initialized with Xavier Normal, representing the core geometric rules of the graph.
    """
    def __init__(self , d_length:int , B:int): 
        super().__init__()
        assert d_length == B

        self.basis_vector_bank = nn.Parameter(torch.empty(B, d_length, d_length))
        nn.init.xavier_normal_(self.basis_vector_bank)


    def forward(self, relation_vector: torch.Tensor, source_features: torch.Tensor, chunk_size: int = 1024) -> torch.Tensor: 
        """
        Computes the edge-specific transformation via a tightly chunked tensor contraction.

        By keeping the chunk size small (e.g., 1024), the ephemeral workspace memory required 
        by PyTorch's einsum backend is strictly bound to ~250 MiB, allowing it to fit into 
        the remaining VRAM on large graphs.
        """
        E = relation_vector.size(0)
        outputs = []
        
        # Iterate over the edges in smaller chunks
        for i in range(0, E, chunk_size):
            rel_chunk = relation_vector[i:i + chunk_size]
            src_chunk = source_features[i:i + chunk_size]
            
            # The intermediate memory spike is now bounded by chunk_size
            chunk_out = torch.einsum("eb, bij, ej -> ei", rel_chunk, self.basis_vector_bank, src_chunk)
            outputs.append(chunk_out)
            
        # Reconstruct the full graph messages
        return torch.cat(outputs, dim=0)