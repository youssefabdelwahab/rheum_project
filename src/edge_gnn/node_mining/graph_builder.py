import torch 
# import nltk 
import spacy
# from sentence_transfomers import SentenceTransformer

# nltk.download('punkt', quiet = True)
nlp = spacy.load("en_core_sci_sm")
print(nlp.meta["version"])



# class DocumentGraphBuilder: 
#     def __init__(self, model_name: str = "NeuML/pubmedbert-base-embeddings"):
#         ""
        
#         self.encoder = SentenceTransformer(model_name)

        

def generate_sentence_nodes(raw_paper_text): 

    doc = nlp(raw_paper_text)
    sentence_nodes = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 10]
    return sentence_nodes
