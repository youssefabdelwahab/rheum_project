from src.edge_gnn.gnn.hole_embedding_layer import Holographic_Edge_Embeddings
from src.edge_gnn.gnn.basis_decomp_layer import Basis_Layer
import torch 
import torch.nn as nn



class Edge_Generator(nn.Module): 
    def __init__(self, d_length: int , hidden_dim: int): 

        super().__init__()

        self.edge_embeddings = Holographic_Edge_Embeddings(d_length, hidden_dim)

        self.basis_vectors = Basis_Layer(d_length, B=d_length)



    def forward(self, edge_index: torch.Tensor , node_embeddings: torch.Tensor): 

        edge_coefficients = self.edge_embeddings(edge_index , node_embeddings)

        edge_matricies = self.basis_vectors(edge_coefficients)
        source_nodes = edge_index[0]
        source_features = node_embeddings[source_nodes]

        transformed_messages = torch.einsum("eij, ej -> ei", edge_matricies, source_features)
        
        return transformed_messages