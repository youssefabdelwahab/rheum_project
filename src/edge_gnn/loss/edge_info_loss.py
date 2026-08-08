import torch
import torch.nn as nn
import torch.nn.functional as F

class EdgeConditionedInfoNCE(nn.Module):
    """
    Computes the InfoNCE contrastive loss for dynamically generated graph edges.
    Forces generated edge messages to align with their true target nodes while
    repelling from all other nodes in the graph.
    """
    def __init__(self, temperature: float = 0.07):
        super().__init__()
        self.temperature = temperature

    def forward(self, transformed_messages: torch.Tensor, all_node_embeddings: torch.Tensor, target_node_indices: torch.Tensor) -> torch.Tensor:
        """
        Args:
            transformed_messages: The output from the HolE Edge_Generator [E, D].
            all_node_embeddings: The full matrix of node features [N, D].
            target_node_indices: The 1D tensor of true destination node IDs [E] (usually edge_index[1]).
            
        Returns:
            Scalar loss tensor.
        """
        # 1. L2 Normalize to ensure dot product acts as strict Cosine Similarity
        # dim=-1 ensures we normalize across the feature vectors, not across the batch
        messages_norm = F.normalize(transformed_messages, p=2, dim=-1)
        nodes_norm = F.normalize(all_node_embeddings, p=2, dim=-1)

        # 2. Compute similarity logits for every message against EVERY node in the graph
        # [E, D] @ [D, N] -> [E, N]
        logits = torch.matmul(messages_norm, nodes_norm.transpose(0, 1))

        # 3. Scale logits by temperature to sharpen the distribution for Cross Entropy
        logits = logits / self.temperature

        # 4. Compute the contrastive loss
        # PyTorch F.cross_entropy expects logits of shape [Batch, Classes] and targets of shape [Batch]
        # Here, Batch = Edges (E), and Classes = Nodes (N)
        loss = F.cross_entropy(logits, target_node_indices)

        return loss