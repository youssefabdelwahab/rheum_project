import torch
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score, average_precision_score
from torch_geometric.utils import homophily

class PipelineEvaluator:
    def __init__(self, random_state: int = 42):
        self.random_state = random_state

    # ==========================================
    # 1. Representation Quality (Linear Probing)
    # ==========================================
    def evaluate_embeddings(self, embeddings: torch.Tensor, labels: torch.Tensor, 
                            train_mask: torch.Tensor, test_mask: torch.Tensor, 
                            multi_label: bool = False) -> dict:
        """
        Trains a linear probe on frozen embeddings to evaluate representational quality.
        """
        # Move data to CPU for Scikit-Learn
        X = embeddings.detach().cpu().numpy()
        y = labels.detach().cpu().numpy()
        
        X_train, y_train = X[train_mask.cpu()], y[train_mask.cpu()]
        X_test, y_test = X[test_mask.cpu()], y[test_mask.cpu()]

        # Initialize linear probe
        clf = LogisticRegression(solver='lbfgs',  
                                 max_iter=1000, random_state=self.random_state)
        
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        # Calculate metrics
        acc = accuracy_score(y_test, y_pred)
        
        if multi_label:
            # For Information Extraction (DocRED/CDR)
            f1_micro = f1_score(y_test, y_pred, average='micro')
            f1_macro = f1_score(y_test, y_pred, average='macro')
            return {"Accuracy": acc, "F1-Micro": f1_micro, "F1-Macro": f1_macro}
        else:
            # For Node Classification (PubMed/WikiCS)
            f1_weighted = f1_score(y_test, y_pred, average='weighted')
            return {"Accuracy": acc, "F1-Weighted": f1_weighted}

    # ==========================================
    # 2. Topological Quality
    # ==========================================
    def evaluate_topology(self, edge_index: torch.Tensor, labels: torch.Tensor, 
                          num_nodes: int) -> dict:
        """
        Measures the structural properties of the generated HolE edge matrices.
        """
        # 1. Edge Homophily (Fraction of edges connecting same-class nodes)
        # Using PyG's native utility for edge homophily ratio
        h_ratio = homophily(edge_index, labels, method='edge')

        # 2. Graph Sparsity (Edges generated vs. total possible edges)
        num_edges = edge_index.size(1)
        max_possible_edges = num_nodes * (num_nodes - 1)
        density = num_edges / max_possible_edges if max_possible_edges > 0 else 0
        sparsity = 1.0 - density

        return {
            "Homophily Ratio": float(h_ratio),
            "Sparsity": sparsity,
            "Total Generated Edges": num_edges
        }

    # ==========================================
    # 3. Structural Link Prediction
    # ==========================================
    def evaluate_link_prediction(self, pos_scores: torch.Tensor, neg_scores: torch.Tensor, k: int = 10) -> dict:
        """
        Evaluates how well the model predicts masked edges vs non-existent edges.
        Expects 1D tensors of logits/probabilities.
        """
        pos_scores = pos_scores.detach().cpu().numpy()
        neg_scores = neg_scores.detach().cpu().numpy()

        labels = np.concatenate([np.ones_like(pos_scores), np.zeros_like(neg_scores)])
        scores = np.concatenate([pos_scores, neg_scores])

        # AUROC and Average Precision (AP)
        auroc = roc_auc_score(labels, scores)
        ap = average_precision_score(labels, scores)

        # Hits@K Calculation
        # Rank all scores; check if positive scores are in the top K
        # This is a simplified pairwise ranking calculation
        threshold = np.sort(scores)[-k]
        hits_at_k = np.mean(pos_scores >= threshold)

        return {
            "AUROC": auroc,
            "Average Precision": ap,
            f"Hits@{k}": hits_at_k
        }


import torch
from torch_geometric.utils import negative_sampling, to_scipy_sparse_matrix
import scipy.sparse as sp

def get_hard_negatives_2hop(edge_index, num_nodes, num_negatives):
    """
    Samples hard negatives that are precisely 2-hops away (share a common neighbor).
    """
    device = edge_index.device
    
    # 1. Convert edge_index to a scipy sparse matrix
    # (To easily compute A^2 for 2-hop paths)
    adj = to_scipy_sparse_matrix(edge_index, num_nodes=num_nodes).tocsr()
    
    # 2. Compute A^2 (Paths of length 2)
    # Non-zero entries here mean two nodes share a common neighbor
    adj_2hop = adj.dot(adj)
    
    # 3. Filter out self-loops and existing 1-hop edges
    # We only want pairs that DO NOT have an edge (adj[u, v] == 0) 
    # but DO have a 2-hop path (adj_2hop[u, v] > 0)
    adj_2hop = adj_2hop.tocoo()
    
    # Extract candidate pairs from A^2
    candidates_row = adj_2hop.row
    candidates_col = adj_2hop.col
    
    # Filter out actual existing edges (1-hop) and self-loops
    # A fast way in scipy is checking against the original adj matrix
    mask = (adj[candidates_row, candidates_col].A1 == 0) & (candidates_row != candidates_col)
    
    hard_row = candidates_row[mask]
    hard_col = candidates_col[mask]
    
    if len(hard_row) == 0:
        # Fallback to standard negative sampling if the graph is too sparse/dense for 2-hop candidates
        return negative_sampling(edge_index, num_nodes=num_nodes, num_neg_samples=num_negatives)
    
    # 4. Randomly sample from our pool of validated 2-hop hard candidates
    if len(hard_row) >= num_negatives:
        idx = np.random.choice(len(hard_row), num_negatives, replace=False)
    else:
        # If we need more than available, sample with replacement or supplement with random
        idx = np.random.choice(len(hard_row), num_negatives, replace=True)
        
    sampled_row = hard_row[idx]
    sampled_col = hard_col[idx]
    
    neg_edge_index = torch.stack([
        torch.tensor(sampled_row, dtype=torch.long),
        torch.tensor(sampled_col, dtype=torch.long)
    ], dim=0).to(device)
    
    return neg_edge_index