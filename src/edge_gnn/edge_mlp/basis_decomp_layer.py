import torch
import torch.nn as nn
import torch.nn.functional as F




class Basis_Layer(nn.Module): 
    def __init__(self , d_length:int , B:int): 
        super().__init__()
        assert d_length == B

        self.basis_vector_bank = nn.Parameter(torch.empty(B, d_length, d_length))
        nn.init.xavier_normal_(self.basis_vector_bank)


    def forward(self, relation_vector: torch.Tensor): 
        return torch.einsum("eb, bij -> eij" , relation_vector , self.basis_vector_bank)
