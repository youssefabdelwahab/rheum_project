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


    def forward(self, relation_vector: torch.Tensor): 
        """
        Computes the edge-specific transformation matrices via tensor contraction.

        Args:
            relation_vector (torch.Tensor): The edge coefficients of shape [E, B], where E is 
                the number of edges in the batch.

        Returns:
            torch.Tensor: The dynamically mixed transformation matrices for each edge, 
                resulting in a shape of [E, d_length, d_length].
        """
        return torch.einsum("eb, bij -> eij" , relation_vector , self.basis_vector_bank)
