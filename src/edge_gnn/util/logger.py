import logging


def setup_logger(log_file: str):
    """Configures a Python logger to write to both the console and a text file."""
    logger = logging.getLogger("GraphTraining")
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers if the function is called multiple times
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        # File Handler
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
        # Console Handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
    return logger