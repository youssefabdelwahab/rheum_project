
import torch 
import torch.nn as nn 
from src.edge_gnn.graph_transformer.transformer import GraphFormer


class GlobalGraphNetwork(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_heads, k_freq, num_layers, num_classes):
        super().__init__()
        
        # 1. Input Projection
        self.input_encoder = nn.Linear(input_dim, hidden_dim)
        
        # 2. The Deep Stack (e.g., 6 layers deep)
        self.layers = nn.ModuleList([
            GraphFormer(hidden_dim, num_heads, k_freq)
            for _ in range(num_layers)
        ])
        
        # 3. Final Task Head
        self.output_head = nn.Linear(hidden_dim, num_classes)

    def forward(self, x, edge_index, wire_coords):
        # Map raw BERT/Text embeddings into the graph space
        x = self.input_encoder(x)
        all_layer_messages = []
        
        # Sequentially pass the graph through all layers
        for layer in self.layers:
            x , layer_messages = layer(x, edge_index, wire_coords)
            all_layer_messages.append(layer_messages)

            
        # Map the final enriched node embeddings to prediction classes
        logits = self.output_head(x)
        
        return logits , all_layer_messages