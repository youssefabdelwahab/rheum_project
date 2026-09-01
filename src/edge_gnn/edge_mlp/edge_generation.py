from src.edge_gnn.edge_mlp.hole_embedding_layer import Holographic_Edge_Embeddings
from src.edge_gnn.edge_mlp.basis_decomp_layer import Basis_Layer
import torch 
import torch.nn as nn



# class Edge_Generator(nn.Module): 
#     """
#     Orchestrates the generation of dynamically transformed edge messages.

#     This module acts as the bridge between the structural node embeddings and the 
#     final message passing step. It first utilizes the Holographic Edge Embeddings 
#     to extract relational coefficients for each edge. It then passes these coefficients 
#     to the Basis Layer to dynamically construct edge-specific transformation matrices. 
#     Finally, it projects the source node features through these custom matrices to 
#     produce the final transformed messages destined for the target nodes.

#     Args:
#         d_length (int): The dimensionality of the node embeddings, edge coefficients, 
#             and basis matrices.
#         hidden_dim (int): The hidden dimension used inside the HolE MLP for refining 
#             the relational signal.

#     Attributes:
#         edge_embeddings (Holographic_Edge_Embeddings): The HolE module that computes 
#             relational coefficients via FFT.
#         basis_vectors (Basis_Layer): The basis dictionary module that maps coefficients 
#             into physical transformation matrices.
#     """
#     def __init__(self, d_length: int , hidden_dim: int): 

#         super().__init__()

#         self.edge_embeddings = Holographic_Edge_Embeddings(d_length, hidden_dim)

#         self.basis_vectors = Basis_Layer(d_length, B=d_length)



#     def forward(self, edge_index: torch.Tensor , node_embeddings: torch.Tensor): 
#         """
#         Executes the full edge message generation pipeline.

#         Args:
#             edge_index (torch.Tensor): A tensor of shape [2, E] defining the graph connectivity.
#             node_embeddings (torch.Tensor): The global node feature matrix of shape [N, d_length].

#         Returns:
#             torch.Tensor: The dynamically transformed messages for each edge, 
#                 resulting in a shape of [E, d_length]. These are ready to be weighted 
#                 by the attention mechanism and aggregated at the target nodes.
#         """

#         edge_coefficients = self.edge_embeddings(edge_index , node_embeddings)

#         edge_matricies = self.basis_vectors(edge_coefficients)
#         source_nodes = edge_index[0]
#         source_features = node_embeddings[source_nodes]

#         transformed_messages = torch.einsum("eij, ej -> ei", edge_matricies, source_features)
        
#         return transformed_messages


class Edge_Generator(nn.Module): 
    """
    Orchestrates the generation of dynamically transformed edge messages.

    This module acts as the bridge between the structural node embeddings and the 
    final message passing step. It first utilizes the Holographic Edge Embeddings 
    to extract relational coefficients for each edge. It then passes these coefficients 
    and the source node features to the Basis Layer to dynamically compute the final 
    transformed messages destined for the target nodes in a single, memory-efficient step.

    Args:
        d_length (int): The dimensionality of the node embeddings, edge coefficients, 
            and basis matrices.
        hidden_dim (int): The hidden dimension used inside the HolE MLP for refining 
            the relational signal.

    Attributes:
        edge_embeddings (Holographic_Edge_Embeddings): The HolE module that computes 
            relational coefficients via FFT.
        basis_vectors (Basis_Layer): The basis dictionary module that maps coefficients 
            and source features into the final message vectors.
    """
    def __init__(self, d_length: int, hidden_dim: int): 
        super().__init__()

        self.edge_embeddings = Holographic_Edge_Embeddings(d_length, hidden_dim)
        self.basis_vectors = Basis_Layer(d_length, B=d_length)

    def forward(self, edge_index: torch.Tensor, node_embeddings: torch.Tensor, previous_edge_state=None): 
        """
        Executes the full edge message generation pipeline.

        Args:
            edge_index (torch.Tensor): A tensor of shape [2, E] defining the graph connectivity.
            node_embeddings (torch.Tensor): The global node feature matrix of shape [N, d_length].

        Returns:
            torch.Tensor: The dynamically transformed messages for each edge, 
                resulting in a shape of [E, d_length]. These are ready to be weighted 
                by the attention mechanism and aggregated at the target nodes.
        """
        # 1. Get relational coefficients [E, d_length]
        edge_coefficients, new_edge_state = self.edge_embeddings(edge_index, node_embeddings, previous_edge_state)

        # 2. Extract source features [E, d_length]
        source_nodes = edge_index[0]
        source_features = node_embeddings[source_nodes]

        # 3. Compute transformed messages directly via associative contraction
        transformed_messages = self.basis_vectors(edge_coefficients, source_features)
        
        return transformed_messages, new_edge_state