"""
VTagger Agent Logger.

Centralized logging for timing diagnostics and debugging.
"""

import time
from datetime import datetime

AGENT_LOG_FILE = "/tmp/vtagger_agent.log"

def log_timing(msg: str):
    """Log message with timestamp to agent log file."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    log_line = f"[{timestamp}] {msg}"
    with open(AGENT_LOG_FILE, "a") as f:
        f.write(log_line + "\n")
    try:
        print(log_line, flush=True)
    except BrokenPipeError:
        pass

def clear_log():
    """Clear the agent log file."""
    with open(AGENT_LOG_FILE, "w") as f:
        f.write("")
