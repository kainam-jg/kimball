"""
KIMBALL Log Pruning Service

This module provides an asynchronous service that periodically prunes old logs
from ClickHouse tables based on TTL configuration.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from .database import DatabaseManager
from .config import Config
from .logger import Logger


class LogPruner:
    """Service that prunes old logs from ClickHouse based on TTL configuration."""
    
    def __init__(self):
        """Initialize the log pruning service."""
        self.db_manager = DatabaseManager()
        self.config = Config()
        self.logger = Logger("log_pruner")
        self.running = False
        self._task: Optional[asyncio.Task] = None
    
    def get_ttl_days(self) -> int:
        """Get TTL configuration in days."""
        return self.config.get('logging.ttl_days', 7)
    
    def get_interval_minutes(self) -> int:
        """Get pruning interval in minutes."""
        admin_config = self.config.get('administration', {})
        pruning_config = admin_config.get('log_pruning', {})
        return pruning_config.get('interval_minutes', 15)
    
    def is_enabled(self) -> bool:
        """Check if log pruning is enabled."""
        admin_config = self.config.get('administration', {})
        pruning_config = admin_config.get('log_pruning', {})
        return pruning_config.get('enabled', True)
    
    async def prune_logs(self) -> Dict[str, Any]:
        """
        Prune logs older than TTL from all log tables.
        
        Returns:
            dict: Pruning results
        """
        try:
            ttl_days = self.get_ttl_days()
            cutoff_date = datetime.now() - timedelta(days=ttl_days)
            
            self.logger.info(f"Starting log pruning: Removing logs older than {cutoff_date} (TTL: {ttl_days} days)")
            
            # All log tables to prune
            log_tables = [
                "logs.application",
                "logs.acquire",
                "logs.discover",
                "logs.model",
                "logs.transform",
                "logs.pipeline",
                "logs.administration"
            ]
            
            results = {}
            total_remaining = 0
            total_deleted = 0
            
            for table_name in log_tables:
                try:
                    # Get count before deletion
                    count_before_sql = f"SELECT count() as total FROM {table_name}"
                    result_before = self.db_manager.execute_query_dict(count_before_sql)
                    count_before = result_before[0]['total'] if result_before else 0
                    
                    # Delete old logs
                    delete_sql = f"""
                    ALTER TABLE {table_name} 
                    DELETE WHERE timestamp < '{cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    """
                    
                    self.db_manager.execute_command(delete_sql)
                    
                    # Get count after deletion (approximate, may not be immediate)
                    count_after_sql = f"SELECT count() as total FROM {table_name}"
                    result_after = self.db_manager.execute_query_dict(count_after_sql)
                    count_after = result_after[0]['total'] if result_after else 0
                    
                    deleted_approx = max(0, count_before - count_after)
                    total_remaining += count_after
                    total_deleted += deleted_approx
                    
                    results[table_name] = {
                        "deleted_approx": deleted_approx,
                        "remaining": count_after
                    }
                    
                except Exception as e:
                    self.logger.warning(f"Error pruning {table_name}: {e}")
                    results[table_name] = {
                        "error": str(e)
                    }
            
            self.logger.info(f"Log pruning completed. Total remaining logs: {total_remaining}, Deleted approx: {total_deleted}")
            
            return {
                "status": "success",
                "ttl_days": ttl_days,
                "cutoff_date": cutoff_date.isoformat(),
                "total_remaining": total_remaining,
                "total_deleted_approx": total_deleted,
                "table_results": results,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error pruning logs: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _pruning_loop(self):
        """Main loop for periodic log pruning."""
        self.logger.info("Log pruning service started")
        
        while self.running:
            try:
                if self.is_enabled():
                    await self.prune_logs()
                else:
                    self.logger.debug("Log pruning is disabled in configuration")
                
                # Wait for next interval
                interval_minutes = self.get_interval_minutes()
                await asyncio.sleep(interval_minutes * 60)
                
            except asyncio.CancelledError:
                self.logger.info("Log pruning service cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in log pruning loop: {e}")
                # Wait a bit before retrying on error
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def start(self):
        """Start the log pruning service."""
        if self.running:
            self.logger.warning("Log pruning service is already running")
            return
        
        self.running = True
        self._task = asyncio.create_task(self._pruning_loop())
        self.logger.info(f"Log pruning service started (interval: {self.get_interval_minutes()} minutes)")
    
    def stop(self):
        """Stop the log pruning service."""
        if not self.running:
            return
        
        self.running = False
        if self._task:
            self._task.cancel()
        self.logger.info("Log pruning service stopped")
    
    async def stop_async(self):
        """Stop the log pruning service (async version)."""
        if not self.running:
            return
        
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.logger.info("Log pruning service stopped")

