import torch 
import torch.nn.functional as F 
from sentence_transformers import SentenceTransformer

MODEL_NAME = "Qwen/Qwen3-Embedding-8B"
device = "cuda" if torch.cuda.is_available() else "cpu"


model = SentenceTransformer(MODEL_NAME, device=device)


def encode_sentence_nodes(sentences: list[str], batch_size: int = 8) -> torch.Tensor:
    """
    Generates L2-normalized semantic embeddings for a list of text nodes.

    This function utilizes a pre-trained SentenceTransformer (e.g., Qwen3-Embedding-8B) 
    to convert raw string sentences into dense numerical vectors. The embeddings are 
    explicitly L2-normalized, which is structurally required for the downstream k-NN 
    graph generator to correctly compute Cosine Similarity[cite: 10]. The final tensor is 
    moved back to the CPU to conserve GPU VRAM for the main graph training loop[cite: 10].

    Args:
        sentences (list[str]): A list of extracted sentence strings representing 
            the individual nodes of the document graph[cite: 10].
        batch_size (int, optional): The number of sentences to process simultaneously 
            through the transformer model. Defaults to 8[cite: 10].

    Returns:
        torch.Tensor: A CPU-bound, dense feature tensor of shape [num_nodes, embedding_dim], 
            where `embedding_dim` matches the specific output dimension of the chosen 
            SentenceTransformer model[cite: 10].
    """
    X = model.encode(
        sentences, 
        batch_size=batch_size, 
        convert_to_tensor=True, 
        normalize_embeddings=True
    )

    return X.cpu()