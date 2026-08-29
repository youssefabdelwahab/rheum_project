import torch
import torch.nn as nn
import torch.nn.functional as F

# class EdgeConditionedInfoNCE(nn.Module):
#     """
#     Computes the Edge Conditioned InfoNCE contrastive loss for dynamically generated graph edges.
#     Forces generated edge messages to align with their true target nodes while
#     repelling from all other nodes in the graph.
#     """
#     def __init__(self, temperature: float = 0.07):
#         super().__init__()
#         self.temperature = temperature

#     def forward(self, transformed_messages: torch.Tensor, all_node_embeddings: torch.Tensor, target_node_indices: torch.Tensor) -> torch.Tensor:
#         """
#         Args:
#             transformed_messages: The output from the HolE Edge_Generator [E, D].
#             all_node_embeddings: The full matrix of node features [N, D].
#             target_node_indices: The 1D tensor of true destination node IDs [E] (usually edge_index[1]).
            
#         Returns:
#             Scalar loss tensor.
#         """

#         transformed_messages = transformed_messages.float()
#         all_node_embeddings = all_node_embeddings.float()
#         # strict Cosine Similarity
#         # dim=-1 ensures we normalize across the feature vectors, not across the batch
#         messages_norm = F.normalize(transformed_messages, p=2, dim=-1)
#         nodes_norm = F.normalize(all_node_embeddings, p=2, dim=-1)

#         #  similarity logits for every message against every node in the graph
#         # [E, D] @ [D, N] -> [E, N]
#         logits = torch.matmul(messages_norm, nodes_norm.transpose(0, 1))

#         # sharpen the distribution for Cross Entropy
#         logits = logits / self.temperature

#         # contrastive loss
#         loss = F.cross_entropy(logits, target_node_indices)

#         return loss


import torch
import torch.nn as nn
import torch.nn.functional as F

class EdgeConditionedInfoNCE(nn.Module):
    """
    Computes the Edge Conditioned InfoNCE contrastive loss using Explicit Negative Sampling.
    Forces generated edge messages to align with their true target nodes while
    repelling strictly from a curated batch of hard and easy negatives.
    """
    def __init__(self, temperature: float = 0.01):
        super().__init__()
        self.temperature = temperature

    def forward(self, messages: torch.Tensor, pos_targets: torch.Tensor, neg_targets: torch.Tensor) -> torch.Tensor:
        """
        Args:
            messages: Output from the Edge_Generator [E, D].
            pos_targets: The true destination node embeddings [E, D].
            neg_targets: The sampled negative node embeddings [E, K, D] (where K is num negatives).
            
        Returns:
            Scalar loss tensor.
        """
        messages = F.normalize(messages.float(), p=2, dim=-1)
        #dataset absolute truth
        pos_targets = F.normalize(pos_targets.float(), p=2, dim=-1)
        #fake edges
        neg_targets = F.normalize(neg_targets.float(), p=2, dim=-1)

        # 1. Positive Similarity: [E, D] * [E, D] -> sum -> [E, 1]

        # compute cosine sim to check if generated message looks like true destination node
        pos_logits = (messages * pos_targets).sum(dim=-1, keepdim=True)

        # 2. Negative Similarity: BMM [E, K, D] @ [E, D, 1] -> [E, K, 1] -> squeeze -> [E, K]

        # tests the fake messages agaisnt nodes it shouldnt be connected to 
        neg_logits = torch.bmm(neg_targets, messages.unsqueeze(2)).squeeze(2)

        # 3. Concatenate Positives and Negatives: [E, 1 + K]
        # Column 0 is the true target, Columns 1 to K are the negatives
        logits = torch.cat([pos_logits, neg_logits], dim=1) / self.temperature

        # 4. Cross Entropy
        # Because we placed the positive target at index 0 for every edge, 
        # the correct label class for all E edges is simply 0.
        labels = torch.zeros(messages.size(0), dtype=torch.long, device=messages.device)

        return F.cross_entropy(logits, labels)