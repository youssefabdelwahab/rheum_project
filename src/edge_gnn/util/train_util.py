import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_learning_curves(csv_path: str, save_dir: str = None):
    """
    Reads the training metrics CSV and generates a 3-panel plot comparing 
    training and validation losses for Total, Task, and InfoNCE metrics.
    
    Args:
        csv_path (str): Path to the metrics.csv file.
        save_dir (str, optional): Directory to save the output image. If None, it just displays.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Could not find log file at {csv_path}")

    # 1. Load the data
    df = pd.read_csv(csv_path)
    epochs = df['epoch']

    # 2. Setup the Matplotlib Figure (1 row, 3 columns)
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle('Model Training Metrics', fontsize=16, fontweight='bold')

    # --- Subplot 1: Total Loss ---
    axes[0].plot(epochs, df['train_total_loss'], label='Train Total', color='blue', linewidth=2)
    if 'val_total_loss' in df.columns and df['val_total_loss'].notna().any():
        axes[0].plot(epochs, df['val_total_loss'], label='Val Total', color='orange', linewidth=2, linestyle='--')
    axes[0].set_title('Total Loss')
    axes[0].set_xlabel('Epoch')
    axes[0].set_ylabel('Loss')
    axes[0].legend()
    axes[0].grid(True, linestyle=':', alpha=0.6)

    # --- Subplot 2: Task Loss (Cross Entropy) ---
    axes[1].plot(epochs, df['train_task_loss'], label='Train Task', color='green', linewidth=2)
    if 'val_task_loss' in df.columns and df['val_task_loss'].notna().any():
        axes[1].plot(epochs, df['val_task_loss'], label='Val Task', color='red', linewidth=2, linestyle='--')
    axes[1].set_title('Task Loss (Supervised)')
    axes[1].set_xlabel('Epoch')
    axes[1].legend()
    axes[1].grid(True, linestyle=':', alpha=0.6)

    # --- Subplot 3: InfoNCE Loss (Self-Supervised) ---
    axes[2].plot(epochs, df['train_nce_loss'], label='Train InfoNCE', color='purple', linewidth=2)
    if 'val_nce_loss' in df.columns and df['val_nce_loss'].notna().any():
        axes[2].plot(epochs, df['val_nce_loss'], label='Val InfoNCE', color='magenta', linewidth=2, linestyle='--')
    axes[2].set_title('InfoNCE Loss (Structural)')
    axes[2].set_xlabel('Epoch')
    axes[2].legend()
    axes[2].grid(True, linestyle=':', alpha=0.6)

    # 3. Clean up layout
    plt.tight_layout()
    plt.subplots_adjust(top=0.88) # Make room for the main title

    # 4. Save or Show
    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, 'learning_curves.png')
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved successfully to {save_path}")
    else:
        plt.show()
        
    # Close the figure to free up memory if running in a loop
    plt.close(fig)

def analyze_latest_run(runs_dir: str = "runs"):
    """
    Utility function that automatically finds the most recent experiment folder 
    in your runs directory and plots its metrics.
    """
    if not os.path.exists(runs_dir):
        print(f"Directory {runs_dir} does not exist.")
        return
        
    # Find all subdirectories in the runs folder
    subdirs = [os.path.join(runs_dir, d) for d in os.listdir(runs_dir) if os.path.isdir(os.path.join(runs_dir, d))]
    
    if not subdirs:
        print("No training runs found.")
        return
        
    # Sort by modification time to get the newest run
    latest_run = max(subdirs, key=os.path.getmtime)
    csv_path = os.path.join(latest_run, "metrics.csv")
    
    if os.path.exists(csv_path):
        print(f"Analyzing latest run: {latest_run}")
        plot_learning_curves(csv_path, save_dir=latest_run)
    else:
        print(f"No metrics.csv found in {latest_run}")