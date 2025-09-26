import os, sys, json, torch , asyncio
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)


from LLM_Agent.batch_inference_temp import load_model_on_gpu, batch_call_llm
from functions.batch_papers import load_batches
from prompts.system_prompts import get_main_clinical_trial

# today_str = datetime.today().strftime('%Y/%m/%d')
# repo_root = Path.cwd().parent
# available_gpus = list(range(torch.cuda.device_count()))
# model = '/work/robust_ai_lab/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2'


# annotation_dir = os.path.join(repo_root, f'research_paper_database/extracted/run_1/annotations/{today_str}')
# os.makedirs(annotation_dir, exist_ok=True)
# output_path = os.path.join(annotation_dir, 'annotated_papers.jsonl')
# input_path = os.path.join(repo_root, 'research_paper_database/extracted/run_1/extracted_paper_info_thread.jsonl')

# list_of_papers = []

# with open(input_path, 'r', encoding='utf-8') as f:
#     for line in f: 
#         row = json.loads(line)
#         paper_info = {key: row.get(key) for key in ['doi', 'paper_text']}
#         list_of_papers.append(paper_info)

# all_papers = [paper['paper_text'] for paper in list_of_papers]
# all_paper_ids = [paper['doi'] for paper in list_of_papers]

# test_papers = all_papers[:4]
# test_paper_ids = all_paper_ids[:4]



async def load_paper_batches(paper_list, paper_id_list, task_queue: asyncio.Queue):
    for paper_ids, papers in load_batches(paper_id_list, paper_list, batch_size=4):
        await task_queue.put(paper_ids, papers) 


async def inference_worker(task_queue: asyncio.Queue, result_queue: asyncio.Queue, model_path: str, gpu_id: int):
    try: 
        generator, sampler,  tokenizer = load_model_on_gpu(model_path=model_path, gpu_id=gpu_id)
        while True:
            ids, texts = await task_queue.get()
            try:
                if ids is None:
                    return
                records = await asyncio.to_thread(
                batch_call_llm, generator, sampler, tokenizer, get_main_clinical_trial, texts, ids
                )
                await result_queue.put(records)  
            finally:
                task_queue.task_done()
    finally: 
        pass
     


async def writer(output_path: str, result_queue: asyncio.Queue):
    with open(output_path, 'a', encoding='utf-8') as output_f:
        while True:
            # updated_rows = []
            records = await result_queue.get()
            try:
                if records is None:
                    return
                for record in records:
                    output_f.write(json.dumps(record) + '\n')
                output_f.flush()

            finally:
                result_queue.task_done()
           
                # doi = record.get("id")
                # with open(input_path, 'r', encoding= 'utf-8') as input_f: 
                #     row = json.loads(line)
                #     if row.get('doi') == doi: 
                #         row['annotated'] = True
                #         with open(input_path, 'w', encoding='utf-8') as updated_input_f:
                #             for row in updated_rows: 
                #                 updated_input_f.write(json.dumps(row) + '\n')

                # updated_rows.append(row)



async def main():
    load_dotenv(os.path.join(parent_folder, 'env.sh'))

    today_str = datetime.today().strftime('%Y/%m/%d')
    repo_root = Path.cwd().parent
    model = '/work/robust_ai_lab/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2'


    annotation_dir = os.path.join(repo_root, f'research_paper_database/extracted/run_1/annotations/{today_str}')
    os.makedirs(annotation_dir, exist_ok=True)
    output_path = os.path.join(annotation_dir, 'annotated_papers.jsonl')
    input_path = os.path.join(repo_root, 'research_paper_database/extracted/run_1/extracted_paper_info_thread.jsonl')

    list_of_papers = []


    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f: 
            row = json.loads(line)
            paper_info = {key: row.get(key) for key in ['doi', 'paper_text']}
            list_of_papers.append(paper_info)

    all_papers = [paper['paper_text'] for paper in list_of_papers]
    all_paper_ids = [paper['doi'] for paper in list_of_papers]

    test_papers = all_papers[:4]
    test_paper_ids = all_paper_ids[:4]


    gpus = list(range(torch.cuda.device_count())) or [0]


    task_q = asyncio.Queue()
    results_q = asyncio.Queue()
    consumer = asyncio.create_task(writer(output_path, results_q))

    workers = [
        asyncio.create_task(inference_worker(task_q, results_q, model, gpu_id=g))
        for g in gpus
    ]

 


    await load_paper_batches(test_paper_ids, test_papers, task_q, batch_size=4)
    for _ in workers:
        await task_q.put((None, None))

    await task_q.join()  # ensure all task_queue items processed

    # for _ in range(num_workers):
    #     await task_q.put((None, None))

    
    # await task_q.join()  # ensure all task_queue items processed
    # for p in producers:
    #     await p


    await results_q.put(None)
    await results_q.join()
    await consumer
    await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())





    # await load_paper_batches(test_paper_ids, test_papers, task_q)
    # producers = [asyncio.create_task(inference_worker(task_q, results_q, model, gpu_id)) for gpu_id in available_gpus]
    # consumer = asyncio.create_task(writer(output_path, results_q))

    # await asyncio.gather(*producers)
    # await results_q.put(None)  # Signal the writer to finish
    # await asyncio.gather(consumer)



# for paper_ids, papers in load_batches(test_papers, test_paper_ids, batch_size=4):
#     gen , tok = load_model_on_gpu(model_path = model, gpu_id =  available_gpus[0])
#     records = batch_call_llm(generator=gen , tokenizer=tok, system_prompt="You are a helpful research assistant that helps people find information.", user_prompts=papers, ids=paper_ids)