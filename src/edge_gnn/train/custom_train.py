import os
import json
import torch
import torch.nn.functional as F
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from src.edge_gnn.util.logger import setup_logger

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
    device: str
):
    """
    Evaluates the model on unseen validation data without tracking gradients.

    This function performs a forward pass across the entire validation dataset to compute 
    multi-task metrics. It dynamically handles the presence or absence of ground truth 
    labels, calculating the structural InfoNCE loss and (if applicable) the supervised 
    cross-entropy task loss. Because it does not track gradients, it is highly memory 
    efficient and strictly used for benchmarking generalization.

    Args:
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
    
    for batch in val_loader:
        batch = batch.to(device)
        
        logits, all_layer_messages, x_hidden = model(batch.x, batch.edge_index, batch.wire_coords)
        
        # InfoNCE Loss Calculation
        nce_loss = sum(info_nce_loss_fn(msg, x_hidden, batch.edge_index[1]) for msg in all_layer_messages) / len(all_layer_messages)
        
        # Task Routing
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
        
    num_batches = len(val_loader)
    avg_val_loss = val_loss / num_batches
    avg_val_nce_loss = val_nce_loss / num_batches
    avg_val_task_loss = (val_task_loss / batches_with_labels) if batches_with_labels > 0 else 0.0
    
    return avg_val_loss, avg_val_task_loss, avg_val_nce_loss

def train_pipeline(
    model: torch.nn.Module,
    train_loader: torch.utils.data.DataLoader,
    optimizer: torch.optim.Optimizer,
    info_nce_loss_fn: torch.nn.Module,
    config: dict,
    val_loader: torch.utils.data.DataLoader = None,
    scheduler: torch.optim.lr_scheduler._LRScheduler = None,  # <-- 1. Add as argument
    run_name: str = "graph_experiment",
    resume_from_checkpoint: str = None
):
    """
    Master orchestrator for the multi-task graph training pipeline.

    This function handles the complete training lifecycle, including dynamic routing 
    between supervised (Task) and self-supervised (InfoNCE) losses, gradient clipping, 
    validation evaluation, metric logging, and atomic checkpointing. It is designed to 
    be fault-tolerant, supporting seamless resumption from saved checkpoints.

    Args:
        model (torch.nn.Module): The GlobalGraphNetwork instance to be trained.
        train_loader (torch.utils.data.DataLoader): PyG DataLoader containing the batched training graphs.
        optimizer (torch.optim.Optimizer): PyTorch optimizer (e.g., AdamW) for weight updates.
        info_nce_loss_fn (torch.nn.Module): The instantiated EdgeConditionedInfoNCE loss module.
        config (dict): Dictionary containing global hyperparameters (e.g., 'device', 'epochs', 'lambda_nce').
        val_loader (torch.utils.data.DataLoader, optional): PyG DataLoader for unseen validation data. 
            If provided, 'checkpoint_best' is saved based on validation loss. Defaults to None.
        scheduler (torch.optim.lr_scheduler._LRScheduler, optional): PyTorch learning rate scheduler 
            (e.g., ReduceLROnPlateau). Note: The actual .step() call for the scheduler must be 
            added to the loop if this is used. Defaults to None.
        run_name (str, optional): Prefix for the generated experiment folder. Defaults to "graph_experiment".
        resume_from_checkpoint (str, optional): Path to a specific '.pt' checkpoint file to seamlessly 
            resume an interrupted training run. Defaults to None.

    Outputs:
        Does not return variables to memory. Generates a timestamped experiment directory containing:
        - config.json: Serialized hyperparameter dictionary.
        - training.log: Timestamped console output history.
        - metrics.csv: Epoch-by-epoch loss tracking for visualization scripts.
        - checkpoint_last.pt: Model and optimizer states from the most recently completed epoch.
        - checkpoint_best.pt: Model and optimizer states from the highest performing epoch.
    """
    device = config.get('device', 'cuda')
    epochs = config.get('epochs', 100)
    lambda_nce = config.get('lambda_nce', 0.1)
    
    #SETUP
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_dir = os.path.join("runs", f"{run_name}_{timestamp}") if not resume_from_checkpoint else os.path.dirname(resume_from_checkpoint)
    os.makedirs(exp_dir, exist_ok=True)
    
    logger = setup_logger(os.path.join(exp_dir, "training.log"))
    logger.info("Initializing Training Pipeline with Validation Support...")
    
    if not resume_from_checkpoint:
        with open(os.path.join(exp_dir, "config.json"), 'w') as f:
            json.dump(config, f, indent=4)
            
    csv_log_file = os.path.join(exp_dir, "metrics.csv")
    
    #RESUMPTION LOGIC
    start_epoch = 1
    best_val_loss = float('inf')  # <-- Now tracks validation loss
    
    if resume_from_checkpoint and os.path.exists(resume_from_checkpoint):
        logger.info(f"Resuming training from checkpoint: {resume_from_checkpoint}")
        checkpoint = torch.load(resume_from_checkpoint, map_location=device)
        model.load_state_dict(checkpoint['model_state_dict'])
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        start_epoch = checkpoint['epoch'] + 1
        best_val_loss = checkpoint.get('best_loss', float('inf'))
        logger.info(f"Successfully loaded model at Epoch {checkpoint['epoch']}")

    model.to(device)
    info_nce_loss_fn.to(device)

    #MASTER LOOP
    for epoch in range(start_epoch, epochs + 1):
        #TRAINING PHASE
        model.train()
        epoch_loss, epoch_task_loss, epoch_nce_loss = 0.0, 0.0, 0.0
        batches_with_labels = 0
        
        progress_bar = tqdm(train_loader, desc=f"Epoch {epoch}/{epochs} [Train]", leave=False)
        
        for batch in progress_bar:
            batch = batch.to(device)
            optimizer.zero_grad()
            
            logits, all_layer_messages, x_hidden = model(batch.x, batch.edge_index, batch.wire_coords)
            
            nce_loss = sum(info_nce_loss_fn(msg, x_hidden, batch.edge_index[1]) for msg in all_layer_messages) / len(all_layer_messages)
            
            if getattr(batch, 'y', None) is not None:
                task_loss = F.cross_entropy(logits, batch.y)
                loss = task_loss + (lambda_nce * nce_loss)
                epoch_task_loss += task_loss.item()
                batches_with_labels += 1
            else:
                loss = nce_loss
                task_loss = torch.tensor(0.0)
                
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            
            epoch_loss += loss.item()
            epoch_nce_loss += nce_loss.item()
            progress_bar.set_postfix({'Loss': f"{loss.item():.4f}"})
            
        # Compile Training Metrics
        num_train_batches = len(train_loader)
        t_loss = epoch_loss / num_train_batches
        t_task = (epoch_task_loss / batches_with_labels) if batches_with_labels > 0 else 0.0
        t_nce = epoch_nce_loss / num_train_batches
        
        #VALIDATION PHASE
        if val_loader is not None:
            v_loss, v_task, v_nce = eval_epoch(model, val_loader, info_nce_loss_fn, lambda_nce, device)
            logger.info(
                f"Epoch {epoch:03d} | "
                f"TRAIN: [Total: {t_loss:.4f}, Task: {t_task:.4f}, NCE: {t_nce:.4f}] | "
                f"VAL: [Total: {v_loss:.4f}, Task: {v_task:.4f}, NCE: {v_nce:.4f}]"
            )
        else:
            v_loss, v_task, v_nce = None, None, None
            logger.info(f"Epoch {epoch:03d} | TRAIN: [Total: {t_loss:.4f}, Task: {t_task:.4f}, NCE: {t_nce:.4f}]")
        
        #METRIC LOGGING FOR VIZ
        log_entry = {
            'epoch': epoch,
            'train_total_loss': t_loss, 'train_task_loss': t_task, 'train_nce_loss': t_nce,
            'val_total_loss': v_loss, 'val_task_loss': v_task, 'val_nce_loss': v_nce
        }
        log_df = pd.DataFrame([log_entry])
        log_df.to_csv(csv_log_file, mode='a', header=not os.path.exists(csv_log_file), index=False)
        
        #CHECKPOINTING
        current_metric = v_loss if v_loss is not None else t_loss
        
        is_best = current_metric < best_val_loss
        if is_best:
            target_name = "Validation" if val_loader is not None else "Training"
            logger.info(f"New best model found! {target_name} Loss decreased from {best_val_loss:.4f} -> {current_metric:.4f}")
            best_val_loss = current_metric
            
        save_checkpoint({
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'best_loss': best_val_loss,
            'config': config
        }, is_best, exp_dir)
        
    logger.info(f"Training complete. Artifacts safely stored in {exp_dir}")