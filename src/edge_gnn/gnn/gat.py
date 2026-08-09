
import torch 
import torch.nn as nn 
from src.edge_gnn.graph_transformer.transformer import GraphFormer


class GlobalGraphNetwork(nn.Module):
    """
    The top-level orchestrator for the multi-task Graph Neural Network.

    This network acts as the global shell for your architecture. It takes raw 
    semantic embeddings (e.g., from Qwen3), maps them into the graph's dynamic 
    hidden dimension, and passes them sequentially through a deep stack of 
    GraphFormer layers. It is explicitly designed for a multi-task training 
    objective: it outputs discrete class logits for supervised classification, 
    while simultaneously returning the raw hidden states and generated messages 
    from every depth level to compute the self-supervised InfoNCE loss.

    Args:
        input_dim (int): The dimensionality of the raw input features 
            (e.g., 768 for Qwen3 embeddings).
        hidden_dim (int): The internal working dimension of the graph layers.
        num_heads (int): The number of parallel attention heads in each layer.
        k_freq (int): The number of Laplacian eigenvectors used for the 
            WIRE spatial routing.
        num_layers (int): The number of stacked GraphFormer blocks.
        num_classes (int): The number of discrete classes for the final 
            supervised node classification task.

    Attributes:
        input_encoder (torch.nn.Linear): Projects raw input features to `hidden_dim`.
        layers (torch.nn.ModuleList): The deep sequence of `GraphFormer` blocks.
        output_head (torch.nn.Linear): Maps the final enriched node embeddings 
            to the target prediction classes.
    """
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
        """
        Executes the full forward pass through the graph network.

        As the node states pass through the deep stack, this method explicitly 
        collects the dynamically generated edge messages from every single layer. 
        This prevents the deeper layers from "forgetting" the topology and allows 
        the InfoNCE loss to regularize the entire network depth.

        Args:
            x (torch.Tensor): Raw node features of shape [N, input_dim].
            edge_index (torch.Tensor): Graph connectivity tensor of shape [2, E].
            wire_coords (torch.Tensor): Laplacian coordinates of shape [N, k_freq].

        Returns:
            tuple: A tuple containing:
                - logits (torch.Tensor): The final classification predictions 
                  of shape [N, num_classes].
                - all_layer_messages (list[torch.Tensor]): A list of length 
                  `num_layers`, where each element is the generated message 
                  tensor of shape [E + N, hidden_dim] for that specific depth.
                - x (torch.Tensor): The final enriched node representations of 
                  shape [N, hidden_dim], extracted immediately before the output 
                  head, used to calculate the InfoNCE cosine similarities.
        """
        # Map raw BERT/Text embeddings into the graph space
        x = self.input_encoder(x)
        all_layer_messages = []
        
        # Sequentially pass the graph through all layers
        for layer in self.layers:
            x , layer_messages = layer(x, edge_index, wire_coords)
            all_layer_messages.append(layer_messages)

            
        # Map the final enriched node embeddings to prediction classes
        logits = self.output_head(x)
        
        return logits , all_layer_messages , x