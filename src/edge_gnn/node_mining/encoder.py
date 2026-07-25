import torch 
import torch.nn.functional as F 
from sentence_transformers import SentenceTransformer

MODEL_NAME = "Qwen/Qwen3-Embedding-8B"
device = "cuda" if torch.cuda.is_available() else "cpu"


model = SentenceTransformer(MODEL_NAME, device=device)


def encode_sentences(sentences: list[str], batch_size: int = 8) -> torch.Tensor: 

    X = model.encode(
        sentences, 
        batch_size=batch_size, 
        convert_to_tensor=True, 
        normalize_embeddings=True
    )

    return X.cpu()