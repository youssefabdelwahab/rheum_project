import torch 
import spacy
from torch_geometric.utils import degree

nlp = spacy.load("en_core_sci_sm")



# class DocumentGraphBuilder: 
#     def __init__(self, model_name: str = "NeuML/pubmedbert-base-embeddings"):
#         ""
        
#         self.encoder = SentenceTransformer(model_name)

        

def generate_sentence_nodes(raw_paper_text): 

    doc = nlp(raw_paper_text)
    sentence_nodes = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 10]
    return sentence_nodes


def sequential_matrix(num_nodes: int, bidirectional:bool = True) -> tuple[torch.Tensor, torch.Tensor]:
    """
    Generate a sequential graph backbone for a given number of nodes.

    Args:
        num_nodes (int): The number of nodes in the graph.
        bidirectional (bool): Whether to create a bidirectional graph. Default is True.

    
    Returns:
        dge_index: Shape [2, E] tensor of source -> target node pairs (PyG standard format).
        adj_matrix: Shape [num_nodes, num_nodes] binary adjacency matrix.
    """

    source_nodes = torch.arange(0 , num_nodes - 1, dtype=torch.long)
    target_nodes = torch.arange(1, num_nodes, dtype=torch.long)

    if bidirectional:
        full_source_nodes = torch.cat([source_nodes, target_nodes])
        full_target_nodes = torch.cat([target_nodes, source_nodes])
    else: 
        full_source_nodes = source_nodes
        full_target_nodes = target_nodes

    edge_index = torch.stack([full_source_nodes, full_target_nodes], dim=0)

    adj_matrix = torch.zeros((num_nodes, num_nodes), dtype=torch.float32)
    adj_matrix[edge_index[0], edge_index[1]] = 1.0
    return edge_index , adj_matrix


def knn_matrix(X: torch.Tensor, k:int = 3) -> torch.Tensor:
    """
    Computes k-nearest neighbor semantic edges based on cosine similarity.
    
    Args:
        X: Feature tensor of shape [num_nodes, embedding_dim] (already L2 normalized).
        k: Number of semantic neighbors to connect per node.
    Returns:
        edge_index_knn: Shape [2, E_knn] tensor of semantic connections.
    """

    num_nodes = X.size(0)

    similarity_matrix = torch.mm(X,X.T)

    similarity_matrix.fill_diagonal_(-float("inf")) 

    _ , topk_indicies = torch.topk(similarity_matrix, k=k, dim=1)

    source_nodes = torch.arange(num_nodes, dtype=torch.long).repeat_interleave(k)
    target_nodes = topk_indicies.reshape(-1)

    edge_index_knn = torch.stack([source_nodes, target_nodes], dim=0)

    return edge_index_knn

def merge_graphs(seq_edge_index:torch.Tensor, knn_edge_index:torch.Tensor) -> torch.Tensor: 
    """
    Combines the sequential backbone and k-NN semantic edges into a single 
    unified graph, automatically deduplicating overlapping edges.
    """

    unified_edges = torch.cat([seq_edge_index, knn_edge_index], dim=1)
    unified_edges = torch.unique(unified_edges, dim=1)

    return unified_edges

def degree_matrix(edge_index: torch.Tensor, num_nodes: int) -> torch.Tensor:
    """
    Computes the sparse normalized degree vector D^{-1/2} used by PyG for 
    symmetric normalization in GCN layers without creating an N x N diagonal grid.
    """
    # Count how many times each node appears as a source in edge_index[0]
    row_degrees = degree(edge_index[0], num_nodes=num_nodes, dtype=torch.float32)

    # Compute D^{-1/2} for symmetric normalization (handling division by zero)
    deg_inv_sqrt = row_degrees.pow(-0.5)
    deg_inv_sqrt[deg_inv_sqrt == float('inf')] = 0.0

    return deg_inv_sqrt