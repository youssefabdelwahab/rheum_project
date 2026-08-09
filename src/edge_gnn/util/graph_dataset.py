import os
import torch
import pandas as pd
from torch_geometric.data import InMemoryDataset, Data
from torch_geometric.nn import knn_graph

# Import your custom modules based on your repository structure
from src.edge_gnn.node_mining.adjancey_matrix import generate_sentence_nodes, sequential_matrix, merge_graphs
from src.edge_gnn.node_mining.encoder import encode_sentence_nodes
from src.edge_gnn.positional_encoding.wire import compute_eigenvectors
from src.edge_gnn.util.doc_cleaning import clean_and_preserve_exact_structure




class Graph_Dataset(InMemoryDataset):
    """
    Orchestrator dataset that parses clinical text, encodes it using Qwen3, 
    builds a unified sequential + semantic graph, computes Laplacian PE, 
    and caches it for highly optimized training.
    """
    def __init__(self, root: str, k_freq: int = 8):
        self.k_freq = k_freq
        
        # We handle all transformations manually in the process() method
        super().__init__(root, transform=None, pre_transform=None)
        
        # Load the cached binary file into RAM
        self.data, self.slices = torch.load(self.processed_paths[0])

  
    @property
    def raw_file_names(self):
        # Dynamically grab every file in the 'root/raw/' directory.
        if not os.path.exists(self.raw_dir):
            return []
        
        # We filter out any hidden system files (like .DS_Store) just in case
        return [f for f in os.listdir(self.raw_dir) if not f.startswith('.')]

    

    @property
    def processed_file_names(self):
        # PyG checks the 'root/processed/' directory for this file to bypass processing
        return ['clinical_graphs_cached.pt']

    def process(self):
        """
        Executes exactly once. Runs the heavy NLP models and matrix operations,
        then serializes everything into a unified PyG binary format.
        """
                  
        data_list = []

        for raw_path in self.raw_file_names: 

            with open(raw_path , 'r', encoding='utf-8') as f: 
                raw_text = f.read()


            cleaned_md = clean_and_preserve_exact_structure(raw_text)
            sentences = generate_sentence_nodes(cleaned_md)
            num_nodes = len(sentences)
            
            # Graphs with fewer than 2 nodes cannot have edges
            if num_nodes < 2:
                continue


            # encode Nodes 
            with torch.no_grad():
                # Returns L2-normalized embeddings of shape [num_nodes, 768/1024/etc.]
                x = encode_sentence_nodes(sentences)

          
            # Structural matrix : Bidirectional sequence (e.g., Sent 0 <-> Sent 1)
            seq_edge_index = sequential_matrix(num_nodes, bidirectional=True)
            
            # Semantic matrix : Connect nodes with similar meaning using PyG's knn_graph.
            knn_edge_index = knn_graph(x, k=3, loop=False, cosine=True)
            
            # Fuse to create final adjancey matrix, deduplicate, and structurally sort the edges
            edge_index = merge_graphs(seq_edge_index, knn_edge_index, num_nodes)

            # get global document postional encoding
            wire_coords = compute_eigenvectors(edge_index, num_nodes, self.k_freq)

          
            

            graph_data = Data(
                x=x, 
                edge_index=edge_index, 
                y=y, 
                wire_coords=wire_coords
            )
                
            data_list.append(graph_data)

        # cache dataset
        data, slices = self.collate(data_list)
        torch.save((data, slices), self.processed_paths[0])
        print(f"Successfully processed and cached {len(data_list)} clinical graphs.")