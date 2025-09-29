


def batched(seq, n): 
    for i in range(0, len(seq), n): 
        yield seq[i:i+n]



def load_batches(all_papers, all_paper_ids, batch_size): 
    
    assert len(all_papers) == len(all_paper_ids), "Length of prompts must match length of ids"
    total = len(all_papers)
    
    pairs = list(zip(all_paper_ids, all_papers))

    for batch in batched(pairs, batch_size): 

        paper_id , paper = zip(*batch)
        return list(paper_id), list(paper)