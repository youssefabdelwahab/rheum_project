import os
import json
import torch
import torch.nn.functional as F
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from src.edge_gnn.util.logger import setup_logger
from src.edge_gnn.util.model_eval import PipelineEvaluator
from src.edge_gnn.util.optimizers import WarmupThenPlateau
from torch.amp import autocast , GradScaler
from torch_geometric.utils import add_self_loops, negative_sampling
from src.edge_gnn.util.model_eval import get_hard_negatives_2hop

def save_checkpoint(state: dict, is_best: bool, save_dir: str):
    """Saves model and optimizer state to disk."""
    os.makedirs(save_dir, exist_ok=True)
    last_path = os.path.join(save_dir, 'checkpoint_last.pt')
    torch.save(state, last_path)
    
    if is_best:
        best_path = os.path.join(save_dir, 'checkpoint_best.pt')
        torch.save(state, best_path)


@torch.no_grad()
def eval_epoch(
    model: torch.nn.Module, 
    val_loader: torch.utils.data.DataLoader, 
    info_nce_loss_fn: torch.nn.Module, 
    lambda_nce: float,
    device: str,
    evaluator = None,
    evaluate_topology: bool = False
):
    """
    Evaluates the model on unseen validation data without tracking gradients.

    This function performs a forward pass across the entire validation dataset to compute 
    multi-task metrics. It dynamically handles the presence or absence of ground truth 
    labels, calculating the structural InfoNCE loss and (if applicable) the supervised 
    cross-entropy task loss. Because it does not track gradients, it is highly memory 
    efficient and strictly used for benchmarking generalization.

    Args:ok
        model (torch.nn.Module): The GlobalGraphNetwork instance to evaluate.
        val_loader (torch.utils.data.DataLoader): PyG DataLoader containing batched validation graphs.
        info_nce_loss_fn (torch.nn.Module): The instantiated EdgeConditionedInfoNCE loss module.
        lambda_nce (float): The scaling weight applied to the InfoNCE loss when combining it 
            with the supervised task loss.
        device (str): The target hardware device (e.g., 'cuda' or 'cpu') to move tensors to.

    Returns:
        tuple: A tuple containing three averaged metrics (floats):
            - avg_val_loss (float): The combined multi-task validation loss.
            - avg_val_task_loss (float): The supervised cross-entropy loss (0.0 if no labels exist).
            - avg_val_nce_loss (float): The self-supervised structural InfoNCE loss.
    
    """
    model.eval()
    
    val_loss = 0.0
    val_task_loss = 0.0
    val_nce_loss = 0.0
    batches_with_labels = 0
    
    all_pos_scores = []
    all_neg_scores = []
    latest_topo_metrics = {}

    for batch in val_loader:
        batch = batch.to(device)
        # gets batch saved attributes here 
        logits, all_layer_messages, x_hidden = model(batch.x, batch.edge_index, batch.wire_coords)
        edge_index_with_loops, _ = add_self_loops(batch.edge_index, num_nodes=batch.x.size(0))
        target_node_indices = edge_index_with_loops[1]

        # --- NEW SAMPLING LOGIC STARTS HERE ---
        num_edges = target_node_indices.size(0)
        num_nodes = x_hidden.size(0)
        
        # 1. Grab Positive Targets
        pos_targets = x_hidden[target_node_indices] 
        
        # 2. Sample Negatives (1 Random Easy, 1 Permuted Hard)
        easy_neg_indices = torch.randint(0, num_nodes, (num_edges,), device=device)
        hard_neg_indices = target_node_indices[torch.randperm(num_edges, device=device)]
        
        # Stack indices to shape [E, 2] and grab embeddings to shape [E, 2, D]
        neg_indices = torch.stack([easy_neg_indices, hard_neg_indices], dim=1)
        neg_targets = x_hidden[neg_indices]

        # Calculate NCE loss using the curated targets
        nce_loss = sum(info_nce_loss_fn(msg, pos_targets, neg_targets) for msg in all_layer_messages) / len(all_layer_messages)
                # --- NEW SAMPLING LOGIC ENDS HERE ---
        
        # 1. InfoNCE Loss Calculation
        # nce_loss = sum(info_nce_loss_fn(msg, x_hidden, target_node_indices) for msg in all_layer_messages) / len(all_layer_messages)
        
        # 2. Task Routing
        if getattr(batch, 'y', None) is not None:
            task_loss = F.cross_entropy(logits, batch.y)
            loss = task_loss + (lambda_nce * nce_loss)
            val_task_loss += task_loss.item()
            batches_with_labels += 1
        else:
            loss = nce_loss
            task_loss = torch.tensor(0.0)
            
        val_loss += loss.item()
        val_nce_loss += nce_loss.item()
        
        # 3. Fast Self-Supervised Link Scoring (Dot-Product Similarity)
        if evaluator is not None:
            pos_edge_index = batch.edge_index
            # neg_edge_index = negative_sampling(
            #     edge_index=pos_edge_index,
            #     num_nodes=batch.num_nodes,
            #     num_neg_samples=pos_edge_index.size(1)
            # )

            neg_edge_index = get_hard_negatives_2hop( 
                edge_index = pos_edge_index, 
                num_nodes = batch.x.size(0), 
                num_negatives = pos_edge_index.size(1)
            )
            neg_edge_index = neg_edge_index.to(device)


            pos_src, pos_dst = pos_edge_index[0], pos_edge_index[1]
            neg_src, neg_dst = neg_edge_index[0], neg_edge_index[1]

            pos_scores = (x_hidden[pos_src] * x_hidden[pos_dst]).sum(dim=-1)
            neg_scores = (x_hidden[neg_src] * x_hidden[neg_dst]).sum(dim=-1)

            all_pos_scores.append(pos_scores.detach().cpu())
            all_neg_scores.append(neg_scores.detach().cpu())

            # 4. Optional Periodic Topological Inspection
            # 4. Optional Periodic Topological Inspection
            if evaluate_topology and getattr(batch, 'y', None) is not None and not latest_topo_metrics:
                
                # 1. Grab final generated messages and corresponding target nodes
                final_messages = all_layer_messages[-1]
                target_nodes_for_topo = x_hidden[edge_index_with_loops[1]]
                
                # 2. Calculate Cosine Similarity to find which edges the model believes are real
                final_messages_norm = F.normalize(final_messages.float(), p=2, dim=-1)
                target_nodes_norm = F.normalize(target_nodes_for_topo.float(), p=2, dim=-1)
                similarity_scores = (final_messages_norm * target_nodes_norm).sum(dim=-1)
                
                # 3. Thresholding: Keep edges with > 50% confidence
                predicted_mask = similarity_scores > 0.5
                
                # 4. Filter down to the model's dynamically predicted topology
                predicted_edge_index = edge_index_with_loops[:, predicted_mask]

                # 5. Evaluate the generated topology, not the ground truth
                latest_topo_metrics = evaluator.evaluate_topology(
                    edge_index=predicted_edge_index,
                    labels=batch.y,
                    num_nodes=batch.x.size(0)
                )
        
    num_batches = len(val_loader)
    avg_val_loss = val_loss / num_batches
    avg_val_nce_loss = val_nce_loss / num_batches
    avg_val_task_loss = (val_task_loss / batches_with_labels) if batches_with_labels > 0 else 0.0
    
    # Aggregate link prediction metrics
    link_metrics = {"AUROC": 0.0, "Average Precision": 0.0, "Hits@10": 0.0}
    if evaluator is not None and len(all_pos_scores) > 0:
        cat_pos = torch.cat(all_pos_scores)
        cat_neg = torch.cat(all_neg_scores)
        link_metrics = evaluator.evaluate_link_prediction(cat_pos, cat_neg, k=10)
    
    return avg_val_loss, avg_val_task_loss, avg_val_nce_loss, link_metrics, latest_topo_metrics

def train_pipeline(
    model: torch.nn.Module,
    train_loader: torch.utils.data.DataLoader,
    optimizer: torch.optim.Optimizer,
    info_nce_loss_fn: torch.nn.Module,
    config: dict,
    val_loader: torch.utils.data.DataLoader = None,
    evaluator = None,
    scheduler: torch.optim.lr_scheduler._LRScheduler = None,
    run_name: str = "graph_experiment",
    resume_from_checkpoint: str = None,
    topo_eval_interval: int = 10
):
    """
    Master orchestrator for the multi-task graph training pipeline with integrated
    self-supervised link prediction validation and periodic topological evaluation.
    """
    device = config.get('device', 'cuda')
    epochs = config.get('epochs', 100)
    lambda_nce = config.get('lambda_nce', 0.1)
    
    # ---------------------------------------------------------
    # Setup Experiment Logging Directory (Routed to Scratch Space)
    # ---------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_run_dir = "/scratch/yous9/rheum_project_runs" # Explicitly routing to scratch
    
    if not resume_from_checkpoint:
        exp_dir = os.path.join(base_run_dir, f"{run_name}_{timestamp}")
    else:
        exp_dir = os.path.dirname(resume_from_checkpoint)
        
    os.makedirs(exp_dir, exist_ok=True)
    # ---------------------------------------------------------
    
    logger = setup_logger(os.path.join(exp_dir, "training.log"))
    logger.info("Initializing Training Pipeline with Validation & Link Prediction Evaluation...")
    
    if not resume_from_checkpoint:
        with open(os.path.join(exp_dir, "config.json"), 'w') as f:
            json.dump(config, f, indent=4)
            
    csv_log_file = os.path.join(exp_dir, "metrics.csv")
    
    # Resumption Logic & Checkpoint Baseline
    start_epoch = 1
    best_val_auc = 0.0
    best_val_loss = float('inf')
    
    if resume_from_checkpoint and os.path.exists(resume_from_checkpoint):
        logger.info(f"Resuming training from checkpoint: {resume_from_checkpoint}")
        checkpoint = torch.load(resume_from_checkpoint, map_location=device)
        model.load_state_dict(checkpoint['model_state_dict'])
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        start_epoch = checkpoint['epoch'] + 1
        best_val_auc = checkpoint.get('best_val_auc', 0.0)
        best_val_loss = checkpoint.get('best_loss', float('inf'))
        logger.info(f"Successfully loaded model at Epoch {checkpoint['epoch']} (Best AUC: {best_val_auc:.4f})")

    model.to(device)
    info_nce_loss_fn.to(device)
    scaler = GradScaler('cuda')


    # Master Training Loop
    for epoch in range(start_epoch, epochs + 1):
        # ---------------- TRAINING PHASE ----------------
        model.train()
        epoch_loss, epoch_task_loss, epoch_nce_loss = 0.0, 0.0, 0.0
        batches_with_labels = 0
        
        progress_bar = tqdm(train_loader, desc=f"Epoch {epoch}/{epochs} [Train]", leave=False)
        
        for batch in progress_bar:
            batch = batch.to(device)
            optimizer.zero_grad()

            with autocast(device_type='cuda', dtype=torch.bfloat16):
                
                # we use the rich X feature that is used to consruct the pos and neg targets that has graph context integrated
                logits, all_layer_messages, x_hidden = model(batch.x, batch.edge_index, batch.wire_coords)
                edge_index_with_loops, _ = add_self_loops(batch.edge_index, num_nodes=batch.x.size(0))
                target_node_indices = edge_index_with_loops[1]
                # --- NEW SAMPLING LOGIC STARTS HERE ---
                # number of items t
                num_edges = target_node_indices.size(0)

                num_nodes = x_hidden.size(0)
                
                # 1. Grab Positive Targets
                pos_targets = x_hidden[target_node_indices] 
                
                # 2. Sample Negatives (1 Random Easy, 1 Permuted Hard)
                easy_neg_indices = torch.randint(0, num_nodes, (num_edges,), device=device)
                hard_neg_indices = target_node_indices[torch.randperm(num_edges, device=device)]
                
                # Stack indices to shape [E, 2] and grab embeddings to shape [E, 2, D]
                neg_indices = torch.stack([easy_neg_indices, hard_neg_indices], dim=1)
                neg_targets = x_hidden[neg_indices]

                # Calculate NCE loss using the curated targets
                nce_loss = sum(info_nce_loss_fn(msg, pos_targets, neg_targets) for msg in all_layer_messages) / len(all_layer_messages)
                # --- NEW SAMPLING LOGIC ENDS HERE ---
            
                # nce_loss = sum(info_nce_loss_fn(msg, x_hidden, target_node_indices) for msg in all_layer_messages) / len(all_layer_messages)
            
                if getattr(batch, 'y', None) is not None:
                    task_loss = F.cross_entropy(logits, batch.y)
                    loss = task_loss + (lambda_nce * nce_loss)
                    epoch_task_loss += task_loss.item()
                    batches_with_labels += 1
                else:
                    loss = nce_loss
                    task_loss = torch.tensor(0.0)
            
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)

            scaler.step(optimizer)
            scaler.update()
            
            epoch_loss += loss.item()
            epoch_nce_loss += nce_loss.item()
            progress_bar.set_postfix({'Loss': f"{loss.item():.4f}"})
            
        num_train_batches = len(train_loader)
        t_loss = epoch_loss / num_train_batches
        t_task = (epoch_task_loss / batches_with_labels) if batches_with_labels > 0 else 0.0
        t_nce = epoch_nce_loss / num_train_batches
        
        # ---------------- VALIDATION PHASE ----------------
        should_eval_topo = (epoch % topo_eval_interval == 0)
        
        if val_loader is not None:
            v_loss, v_task, v_nce, link_metrics, topo_metrics = eval_epoch(
                model=model,
                val_loader=val_loader,
                info_nce_loss_fn=info_nce_loss_fn,
                lambda_nce=lambda_nce,
                device=device,
                evaluator=evaluator,
                evaluate_topology=should_eval_topo
            )
            
            val_auc = link_metrics.get("AUROC", 0.0)
            val_ap = link_metrics.get("Average Precision", 0.0)
            
            logger.info(
                f"Epoch {epoch:03d} | "
                f"TRAIN: [Total: {t_loss:.4f}, NCE: {t_nce:.4f}] | "
                f"VAL: [Total: {v_loss:.4f}, AUC: {val_auc:.4f}, AP: {val_ap:.4f}]"
            )
            
            if should_eval_topo and topo_metrics:
                logger.info(f"Epoch {epoch:03d} Topology Quality: {topo_metrics}")
        else:
            v_loss, v_task, v_nce, val_auc, val_ap = None, None, None, 0.0, 0.0
            logger.info(f"Epoch {epoch:03d} | TRAIN: [Total: {t_loss:.4f}, Task: {t_task:.4f}, NCE: {t_nce:.4f}]")
        
        # Learning Rate Scheduler Step
        # if scheduler is not None:
        #     if isinstance(scheduler, torch.optim.lr_scheduler.ReduceLROnPlateau):
        #         scheduler.step(v_loss if v_loss is not None else t_loss)
        #     else:
        #         scheduler.step()

       # Learning Rate Scheduler Step
        if scheduler is not None:
            # Our custom wrapper or standard ReduceLROnPlateau both take the metric step
            if isinstance(scheduler, WarmupThenPlateau) or isinstance(scheduler, torch.optim.lr_scheduler.ReduceLROnPlateau):
                # Pass validation AUROC or fallback to total loss
                scheduler.step(val_auc if val_auc > 0.0 else (v_loss if v_loss is not None else t_loss))
            else:
                scheduler.step()
        # ---------------- METRIC LOGGING ----------------
        log_entry = {
            'epoch': epoch,
            'train_total_loss': t_loss, 'train_task_loss': t_task, 'train_nce_loss': t_nce,
            'val_total_loss': v_loss, 'val_task_loss': v_task, 'val_nce_loss': v_nce,
            'val_auc': val_auc, 'val_ap': val_ap
        }
        log_df = pd.DataFrame([log_entry])
        log_df.to_csv(csv_log_file, mode='a', header=not os.path.exists(csv_log_file), index=False)
        
        # ---------------- CHECKPOINTING ----------------
        # Prioritize Link Prediction AUROC if available, fallback to validation loss
        if evaluator is not None and val_loader is not None:
            is_best = val_auc > best_val_auc
            if is_best:
                logger.info(f"New best model found! Validation AUROC increased from {best_val_auc:.4f} -> {val_auc:.4f}")
                best_val_auc = val_auc
        else:
            current_loss = v_loss if v_loss is not None else t_loss
            is_best = current_loss < best_val_loss
            if is_best:
                logger.info(f"New best model found! Loss decreased from {best_val_loss:.4f} -> {current_loss:.4f}")
                best_val_loss = current_loss
                
        save_checkpoint({
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'best_val_auc': best_val_auc,
            'best_loss': best_val_loss,
            'config': config
        }, is_best, exp_dir)
        
    logger.info(f"Training complete. Best checkpoints and logs saved in: {exp_dir}")