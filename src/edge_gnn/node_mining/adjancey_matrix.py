import torch 
import spacy
from torch_geometric.utils import coalesce, to_undirected
nlp = spacy.load("en_core_sci_sm")



        
def generate_sentence_nodes(raw_paper_text): 
    """
    Parses raw clinical text into a list of discrete sentence nodes.

    This function utilizes the 'en_core_sci_sm' spaCy model to perform accurate 
    sentence boundary detection on scientific and clinical text. It automatically 
    strips leading/trailing whitespace and filters out any sentences that are 10 
    characters or shorter to remove noise (e.g., stray punctuation or isolated numbers).

    Args:
        raw_paper_text (str): The raw, unformatted text of the document.

    Returns:
        list[str]: A list of cleaned sentence strings, where each string represents 
            a single node in the document graph.
    """

    doc = nlp(raw_paper_text)
    sentence_nodes = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 10]
    return sentence_nodes


def sequential_matrix(num_nodes: int, bidirectional: bool = True) -> torch.Tensor:
    """
    Generates a structural backbone graph connecting sequential nodes.

    This creates the foundational physical topology of the document, where sentence i 
    is connected to sentence i+1 (e.g., 0 -> 1, 1 -> 2). This allows the network to 
    understand the natural reading order of the text.

    Args:
        num_nodes (int): The total number of sentence nodes in the document.
        bidirectional (bool, optional): If True, edges are mirrored so that information 
            can flow both forwards and backwards (e.g., 0 <-> 1). Defaults to True.

    Returns:
        torch.Tensor: A sparse graph connectivity tensor (edge_index) of shape [2, E], 
            where E is the number of sequential edges.
    """
 
    # forward sequential edges (0->1, 1->2, etc.)
    source_nodes = torch.arange(0, num_nodes - 1, dtype=torch.long)
    target_nodes = torch.arange(1, num_nodes, dtype=torch.long)
    edge_index = torch.stack([source_nodes, target_nodes], dim=0)

    # bidirectional mirroring 
    if bidirectional:
        edge_index = to_undirected(edge_index, num_nodes=num_nodes)

    return edge_index


def merge_graphs(seq_edge_index: torch.Tensor, knn_edge_index: torch.Tensor, num_nodes: int) -> torch.Tensor:
    """
    Fuses multiple edge indices into a single, optimized graph topology.

    This function combines the structural backbone (sequential edges) and the semantic 
    shortcuts (k-NN edges). It utilizes PyTorch Geometric's `coalesce` function to 
    automatically remove any duplicate/overlapping edges and sort the resulting matrix 
    by source node, which is required for efficient message passing and scatter operations.

    Args:
        seq_edge_index (torch.Tensor): The sequential edge index of shape [2, E_seq].
        knn_edge_index (torch.Tensor): The semantic k-NN edge index of shape [2, E_knn].
        num_nodes (int): The total number of nodes in the graph (used by coalesce for 
            bounds checking and proper sorting).

    Returns:
        torch.Tensor: A single, deduplicated, and sorted edge index of shape [2, E_total] 
            representing the unified graph topology.
    """
 
    unified_edges = torch.cat([seq_edge_index, knn_edge_index], dim=1)
    unified_edges = coalesce(unified_edges, num_nodes=num_nodes)

    return unified_edges