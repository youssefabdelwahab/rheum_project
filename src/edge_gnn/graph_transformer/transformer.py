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

    This block represents a single layer in the deep graph network. It follows a 
    Pre-Norm transformer architecture. During the forward pass, it completely 
    decouples the message routing (calculated geometrically via WIRE attention) 
    from the message content (generated dynamically via HolE and basis decomposition). 
    The dynamically generated messages are scaled by their geometric attention 
    probabilities, aggregated at the target nodes, and passed through a standard 
    Feed-Forward Network (FFN) to update the node states.

    Args:
        hidden_dim (int): The dimensionality of the input and output node features.
        num_heads (int): The number of parallel attention heads. `hidden_dim` must 
            be divisible by this value.
        k_freq (int): The number of Laplacian eigenvectors used for spatial routing.
        mlp_multiplier (int, optional): The expansion factor for the internal 
            Feed-Forward Network. Defaults to 4.

    Attributes:
        norm1 (torch.nn.LayerNorm): Pre-norm applied before attention and message generation.
        attention_router (MultiHead_Attention): Module that computes edge probabilities 
            using structural wire coordinates.
        edge_generator (Edge_Generator): Module that computes the custom transformation 
            matrices and generated messages for each edge.
        W_o (torch.nn.Linear): Linear projection applied after multi-head message aggregation.
        norm2 (torch.nn.LayerNorm): Pre-norm applied before the Feed-Forward Network.
        mlp (torch.nn.Sequential): 2-layer FFN with GELU activation and dropout.
    """

    def __init__(self, hidden_dim: int, num_heads: int, k_freq: int, mlp_multiplier: int = 4):
        super().__init__()

        self.hidden_dim = hidden_dim
        self.num_heads = num_heads
        self.head_dim = hidden_dim // num_heads

        self.norm1 = nn.LayerNorm(hidden_dim)

        self.attention_router = MultiHead_Attention( 
            in_features=hidden_dim,
            out_features=hidden_dim,
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


    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, wire_coords: torch.Tensor, previous_edge_state=None):
        """
        Executes a single layer of graph message passing and node state updates.

        Self-loops are automatically added to the graph topology at the beginning 
        of the pass to ensure nodes retain their own information during aggregation. 

        Args:
            x (torch.Tensor): The current node feature matrix of shape [N, hidden_dim].
            edge_index (torch.Tensor): The graph connectivity tensor of shape [2, E].
            wire_coords (torch.Tensor): The global spatial coordinates of shape [N, k_freq].

        Returns:
            tuple: A tuple containing:
                - x (torch.Tensor): The updated node features of shape [N, hidden_dim].
                - transformed_messages (torch.Tensor): The raw generated messages for 
                  every edge (including self-loops) of shape [E + N, hidden_dim]. 
                  These are passed up to the global network to compute the InfoNCE loss.
        """

        num_nodes = x.size(0)
        edge_index, _ = add_self_loops(edge_index, num_nodes=num_nodes)
        target_nodes = edge_index[1]

        h1 = self.norm1(x)

        attention_weights = self.attention_router(h1, edge_index, wire_coords)

        transformed_messages ,current_edge_state = self.edge_generator(edge_index, h1, previous_edge_state)

        multi_head_messages = transformed_messages.view(-1, self.num_heads, self.head_dim)

        weighted_messages = multi_head_messages * attention_weights.unsqueeze(-1)

        #aggregation of messages after weighting them 
        agg_buffer = torch.zeros(num_nodes, self.num_heads, self.head_dim, device=x.device)
        scatter_add(weighted_messages, target_nodes, dim=0, out=agg_buffer)
    
        # Flatten and project
        agg_flat = agg_buffer.view(num_nodes, self.hidden_dim)
        out1 = self.W_o(agg_flat)
        
        x = x + out1


        
        # Pre-Norm 2
        h2 = self.norm2(x)
        
        # Feed-Forward Network
        out2 = self.mlp(h2)
        
        # Second Residual Connection
        x = x + out2
        
        return x , transformed_messages , current_edge_state

