import torch 
import torch.nn as nn 
import math 
from torch_geometric.utils import add_self_loops
from torch_scatter import scatter_add
from src.edge_gnn.attention.multihead_attn import MultiHead_Attention
from src.edge_gnn.edge_mlp.edge_generation import Edge_Generator


class GraphFormer(nn.Module):
    """
    Unified Graph Transformer Block integrating Holographic Edge Generation 
    with Wave-Induced Rotary Embeddings (WIRE) Attention.
    """

    def __init__(self, hidden_dim: int, num_heads: int, k_freq: int, mlp_multiplier: int = 4):
        super().__init__()

        self.hidden_dim = hidden_dim
        self.num_heads = num_heads
        self.head_dim = hidden_dim // num_heads

        self.norm1 = nn.LayerNorm(hidden_dim)

        self.attention_router = MultiHead_Attention( 
            in_features=hidden_dim,
            hidden_dim=hidden_dim,
            num_heads=num_heads,
            k_freq=k_freq
        )

        self.edge_generator = Edge_Generator(
            d_length=hidden_dim,
            hidden_dim=hidden_dim*2
        )

        self.W_o = nn.Linear(hidden_dim, hidden_dim, bias=False)

        self.norm2 = nn.LayerNorm(hidden_dim)

        self.mlp = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * mlp_multiplier),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim * mlp_multiplier, hidden_dim)
        )


    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, wire_coords: torch.Tensor): 

        num_nodes = x.size(0)
        edge_index, _ = add_self_loops(edge_index, num_nodes=num_nodes)
        target_nodes = edge_index[1]

        h1 = self.norm1(x)

        attention_weights = self.attention_router(h1, edge_index, wire_coords)

        transformed_messages = self.edge_generator(edge_index, h1)

        multi_head_messages = transformed_messages.view(-1, self.num_heads, self.head_dim)

        weighted_messages = multi_head_messages * attention_weights.unsqueeze(-1)

        agg_buffer = torch.zeros(num_nodes, self.num_heads, self.head_dim, device=x.device)
        scatter_add(weighted_messages, target_nodes, dim=0, out=agg_buffer)
    
        # Flatten and project
        agg_flat = agg_buffer.view(num_nodes, self.hidden_dim)
        out1 = self.W_o(agg_flat)
        
        x = x + out1


        
        # Pre-Norm 2
        h2 = self.norm2(x)
        
        # Process through Feed-Forward Network
        out2 = self.mlp(h2)
        
        # The Second Residual Connection
        x = x + out2
        
        return x , transformed_messages

