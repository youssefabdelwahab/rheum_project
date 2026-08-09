import torch
import torch.nn as nn
import torch.nn.functional as F

class EdgeConditionedInfoNCE(nn.Module):
    """
    Computes the Edge Conditioned InfoNCE contrastive loss for dynamically generated graph edges.
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
        # strict Cosine Similarity
        # dim=-1 ensures we normalize across the feature vectors, not across the batch
        messages_norm = F.normalize(transformed_messages, p=2, dim=-1)
        nodes_norm = F.normalize(all_node_embeddings, p=2, dim=-1)

        #  similarity logits for every message against every node in the graph
        # [E, D] @ [D, N] -> [E, N]
        logits = torch.matmul(messages_norm, nodes_norm.transpose(0, 1))

        # sharpen the distribution for Cross Entropy
        logits = logits / self.temperature

        # contrastive loss
        loss = F.cross_entropy(logits, target_node_indices)

        return loss