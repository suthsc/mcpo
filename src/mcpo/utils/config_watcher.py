import asyncio
import json
import logging
from pathlib import Path
from typing import Callable, Optional, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent


logger = logging.getLogger(__name__)


class ConfigChangeHandler(FileSystemEventHandler):
    """Handler for config file changes."""
    
    def __init__(self, config_path: Path, reload_callback: Callable[[Dict[str, Any]], None]):
        self.config_path = config_path
        self.reload_callback = reload_callback
        self._last_modification = 0
        self._debounce_delay = 0.5  # 500ms debounce
        
    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return
            
        # Check if the modified file is our config file
        if Path(event.src_path) == self.config_path:
            current_time = asyncio.get_event_loop().time()
            
            # Debounce rapid file changes
            if current_time - self._last_modification < self._debounce_delay:
                return
                
            self._last_modification = current_time
            
            # Schedule the reload callback
            asyncio.create_task(self._handle_config_change())
    
    async def _handle_config_change(self):
        """Handle config change with proper error handling."""
        try:
            await asyncio.sleep(self._debounce_delay)  # Additional debounce
            
            logger.info(f"Config file changed: {self.config_path}")
            
            # Read and validate the new config
            with open(self.config_path, 'r') as f:
                new_config = json.load(f)
            
            # Call the reload callback
            await self.reload_callback(new_config)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_path}")
        except Exception as e:
            logger.error(f"Error reloading config: {e}")


class ConfigWatcher:
    """Watches a config file for changes and triggers reloads."""
    
    def __init__(self, config_path: str, reload_callback: Callable[[Dict[str, Any]], None]):
        self.config_path = Path(config_path).resolve()
        self.reload_callback = reload_callback
        self.observer: Optional[Observer] = None
        self.handler: Optional[ConfigChangeHandler] = None
        
    def start(self):
        """Start watching the config file."""
        if not self.config_path.exists():
            logger.error(f"Config file does not exist: {self.config_path}")
            return
        
        self.handler = ConfigChangeHandler(self.config_path, self.reload_callback)
        self.observer = Observer()
        
        # Watch the directory containing the config file
        watch_dir = self.config_path.parent
        self.observer.schedule(self.handler, str(watch_dir), recursive=False)
        
        self.observer.start()
        logger.info(f"Started watching config file: {self.config_path}")
    
    def stop(self):
        """Stop watching the config file."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info(f"Stopped watching config file: {self.config_path}")
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()