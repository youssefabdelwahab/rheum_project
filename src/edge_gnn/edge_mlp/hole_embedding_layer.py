import torch
import torch.nn as nn
import torch.nn.functional as F





class Holographic_Edge_Embeddings(nn.Module):

    def __init__(self, d_length:int , hidden_dim: int): 
        super().__init__()
        self.d = d_length 
        self.mlp = nn.Sequential( 
            nn.Linear(d_length, hidden_dim), 
            nn.GELU(), 
            nn.Dropout(0.1), 
            nn.Linear(hidden_dim, d_length)
        )
        self.layer_norm = nn.LayerNorm(d_length)

    def adjacey_matrix_lookup(self ,edge_index: torch.Tensor , node_embeddings: torch.Tensor): 

        source_nodes = edge_index[0]
        target_nodes = edge_index[1]

        source_node_embeddings = node_embeddings[source_nodes].contiguous()
        target_node_embeddings = node_embeddings[target_nodes].contiguous()

        return source_node_embeddings, target_node_embeddings

    def forward(self, edge_index: torch.Tensor, node_embeddings: torch.Tensor): 

        source_node_embeddings, target_node_embeddings = self.adjacency_matrix_lookup(edge_index, node_embeddings)

        original_dtype = source_node_embeddings.dtype 

        source_32 = source_node_embeddings.to(torch.float32)
        target_32 = target_node_embeddings.to(torch.float32)

        fft_source_32 = torch.fft.rfft(source_32, dim=-1)
        fft_target_32 = torch.fft.rfft(target_32, dim=-1)

        raw_freq = torch.conj(fft_source_32) * fft_target_32

        real_freq = torch.fft.irfft(raw_freq, n=self.d, dim=-1)

        final_edge_vector = real_freq.to(original_dtype)
        
        output = self.mlp(final_edge_vector)
        return self.layer_norm(output)