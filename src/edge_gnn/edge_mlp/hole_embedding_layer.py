import torch
import torch.nn as nn
import torch.nn.functional as F





class Holographic_Edge_Embeddings(nn.Module):
    """
    Generates dynamic relational edge embeddings using Holographic (HolE) circular correlation.

    This module captures the structural and semantic relationship between connected nodes 
    by mapping their features into the frequency domain via Fast Fourier Transform (FFT). 
    It computes the circular correlation between the source and target nodes, transforms 
    the signal back to the spatial domain, and refines it through a Feed-Forward Network. 
    The output serves as the relational coefficients used to mix the basis matrices in 
    the subsequent Basis Layer.

    Args:
        d_length (int): The dimensionality of the input node embeddings and the final 
            output edge coefficients.
        hidden_dim (int): The hidden dimensionality of the internal refining MLP.

    Attributes:
        d (int): Stored dimension length required for the inverse FFT reconstruction.
        mlp (torch.nn.Sequential): A 2-layer network that interprets and refines the 
            raw Fourier correlation signal.
        layer_norm (torch.nn.LayerNorm): Stabilizes the final edge coefficients.
    """

    def __init__(self, d_length:int , hidden_dim: int): 
        super().__init__()
        self.d = d_length 
        self.memory_projector = nn.Linear(d_length, d_length)

        self.mlp = nn.Sequential( 
            nn.Linear(d_length, hidden_dim), 
            nn.GELU(), 
            nn.Dropout(0.1), 
            nn.Linear(hidden_dim, d_length)
        )
        self.layer_norm = nn.LayerNorm(d_length)

    def adjacency_matrix_lookup(self ,edge_index: torch.Tensor , node_embeddings: torch.Tensor):
        """
        Extracts the feature vectors for the source and target nodes of every edge.

        Args:
            edge_index (torch.Tensor): A tensor of shape [2, E] representing the graph connectivity.
            node_embeddings (torch.Tensor): The full matrix of node features of shape [N, d_length].

        Returns:
            tuple: A tuple containing two tensors:
                - source_node_embeddings (torch.Tensor): Features of the sending nodes, shape [E, d_length].
                - target_node_embeddings (torch.Tensor): Features of the receiving nodes, shape [E, d_length].
        """

        source_nodes = edge_index[0]
        target_nodes = edge_index[1]

        source_node_embeddings = node_embeddings[source_nodes].contiguous()
        target_node_embeddings = node_embeddings[target_nodes].contiguous()

        return source_node_embeddings, target_node_embeddings

    def forward(self, edge_index: torch.Tensor, node_embeddings: torch.Tensor, previous_edge_state=None): 
        """
        Computes the refined holographic relation vector for each edge in the graph.

        The operation explicitly casts vectors to float32 to ensure numerical stability 
        during the FFT and inverse FFT operations, before safely restoring the original 
        datatype (useful if training in FP16/BF16 mixed precision).

        Args:
            edge_index (torch.Tensor): A tensor of shape [2, E] detailing graph connectivity.
            node_embeddings (torch.Tensor): Node features of shape [N, d_length].

        Returns:
            torch.Tensor: The normalized relational coefficients for each edge, 
                yielding a shape of [E, d_length].
        """

        source_node_embeddings, target_node_embeddings = self.adjacency_matrix_lookup(edge_index, node_embeddings)

        original_dtype = source_node_embeddings.dtype 

        source_32 = source_node_embeddings.to(torch.float32)
        target_32 = target_node_embeddings.to(torch.float32)

        fft_source_32 = torch.fft.rfft(source_32, dim=-1)
        fft_target_32 = torch.fft.rfft(target_32, dim=-1)

        raw_freq = torch.conj(fft_source_32) * fft_target_32

        real_freq = torch.fft.irfft(raw_freq, n=self.d, dim=-1)

        final_edge_vector = real_freq.to(original_dtype)

        # Integrate the memory from the previous GNN layer
        if previous_edge_state is not None: 
            final_edge_vector = final_edge_vector + self.memory_projector(previous_edge_state)
        
        output = self.mlp(final_edge_vector)
        return self.layer_norm(output), final_edge_vector