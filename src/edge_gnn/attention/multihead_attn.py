import torch 
import math
import torch.nn as nn 
from torch_geometric.utils import add_self_loops, softmax 
from torch_scatter import scatter_add 
from src.edge_gnn.positional_encoding.wire import WireEngine

class MultiHead_Attention(nn.Module): 
    """
    Multi-Head Graph Attention layer with Wave-Induced Rotary Embeddings (WIRE).
    This layer routes features across a graph strictly using defined edges, 
    injecting global topological geometry into the attention scores.
    """
    def __init__(self, in_features: int, out_features: int, num_heads: int, k_freq: int): 
        super().__init__()

        assert out_features % num_heads == 0, "out_features must be divisible by num_heads"
        
        self.num_heads = num_heads
        self.out_features = out_features
        self.head_dim = out_features // num_heads 

        self.W_q = nn.Linear(in_features, out_features, bias=False)
        self.W_k = nn.Linear(in_features, out_features, bias=False)
        self.W_v = nn.Linear(in_features, out_features, bias=False)
        self.W_o = nn.Linear(out_features, out_features, bias=False)

        self.wire_engine = WireEngine(k_freq=k_freq, d_attention=self.head_dim)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, wire_coordinates: torch.Tensor) -> torch.Tensor: 
        """
        Executes the sparse graph attention forward pass.
        
        Args:
            x: Input node features of shape [num_nodes, in_features].
            edge_index: Graph connectivity tensor of shape [2, num_edges].
            wire_coordinates: Laplacian eigenvectors of shape [num_nodes, k_freq].
            
        Returns:
            Updated node features of shape [num_nodes, out_features].
        """
        num_nodes = x.size(0)
        
        edge_index, _ = add_self_loops(edge_index, num_nodes=num_nodes)
        
        source_nodes, target_nodes = edge_index[0], edge_index[1]

        q = self.W_q(x).view(num_nodes, self.num_heads, self.head_dim)
        k = self.W_k(x).view(num_nodes, self.num_heads, self.head_dim)
        v = self.W_v(x).view(num_nodes, self.num_heads, self.head_dim)

        sin, cos = self.wire_engine(wire_coordinates)

        sin = sin.unsqueeze(1)
        cos = cos.unsqueeze(1)

        q_rotated = self.wire_engine.rotate(q, sin, cos)
        k_rotated = self.wire_engine.rotate(k, sin, cos)

        q_target = q_rotated[target_nodes]
        k_source = k_rotated[source_nodes]
        v_source = v[source_nodes]

        attention_scores = (q_target * k_source).sum(dim=-1) / math.sqrt(self.head_dim)

        attention_weights = softmax(attention_scores, target_nodes, num_nodes=num_nodes)
        
        weighted_values = v_source * attention_weights.unsqueeze(-1)
        
        out = torch.zeros(num_nodes, self.num_heads, self.head_dim, device=x.device)
        scatter_add(weighted_values, target_nodes, dim=0, out=out)
        
        out = out.view(num_nodes, self.out_features)
        
        return self.W_o(out)