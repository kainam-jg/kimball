"""
KIMBALL Scheduler Service

This module provides scheduling capabilities for Data Contracts and Pipelines.
Supports time-based scheduling (daily, hourly, weekly, etc.) and event-based triggering.

Note: This is a foundation for future scheduler service implementation.
The actual scheduler daemon/service will be built separately.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from enum import Enum

from .database import DatabaseManager
from .logger import Logger
from ..acquire.data_contract_manager import DataContractManager
from ..acquire.stage0_engine import Stage0Engine


class ScheduleFrequency(str, Enum):
    """Supported schedule frequencies."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ON_DEMAND = "on_demand"


class SchedulerService:
    """Service for scheduling and executing Data Contracts based on frequency."""
    
    def __init__(self):
        """Initialize the scheduler service."""
        self.db_manager = DatabaseManager()
        self.logger = Logger("scheduler_service")
        self.contract_manager = DataContractManager()
        self.stage0_engine = Stage0Engine()
    
    def get_contracts_by_frequency(self, frequency: str) -> List[Dict[str, Any]]:
        """
        Get all Data Contracts scheduled for a specific frequency.
        
        Args:
            frequency: Execution frequency (hourly, daily, weekly, monthly)
            
        Returns:
            list: Contracts with the specified frequency
        """
        try:
            contracts = self.contract_manager.list_contracts(execution_frequency=frequency)
            return contracts
            
        except Exception as e:
            self.logger.error(f"Error getting contracts by frequency {frequency}: {e}")
            return []
    
    def get_all_scheduled_contracts(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all contracts grouped by execution frequency.
        
        Returns:
            dict: Contracts grouped by frequency
        """
        try:
            all_contracts = self.contract_manager.list_contracts()
            
            grouped = {
                "hourly": [],
                "daily": [],
                "weekly": [],
                "monthly": [],
                "on_demand": []
            }
            
            for contract in all_contracts:
                freq = contract.get('execution_frequency', 'on_demand').lower()
                if freq in grouped:
                    grouped[freq].append(contract)
            
            return grouped
            
        except Exception as e:
            self.logger.error(f"Error getting all scheduled contracts: {e}")
            return {}
    
    async def execute_scheduled_contracts(self, frequency: str) -> Dict[str, Any]:
        """
        Execute all contracts scheduled for a specific frequency.
        
        This method will be called by the scheduler daemon/service.
        
        Args:
            frequency: Execution frequency to execute
            
        Returns:
            dict: Execution results
        """
        try:
            contracts = self.get_contracts_by_frequency(frequency)
            
            if not contracts:
                return {
                    "frequency": frequency,
                    "contracts_executed": 0,
                    "status": "success",
                    "message": f"No contracts scheduled for {frequency} execution"
                }
            
            results = []
            for contract in contracts:
                transformation_id = contract['transformation_id']
                try:
                    result = await self.stage0_engine.execute_contract(transformation_id)
                    results.append({
                        "transformation_id": transformation_id,
                        "transformation_name": contract['transformation_name'],
                        "status": result.get('status', 'unknown'),
                        "records_loaded": result.get('records_loaded', 0)
                    })
                except Exception as e:
                    self.logger.error(f"Error executing contract {transformation_id}: {e}")
                    results.append({
                        "transformation_id": transformation_id,
                        "transformation_name": contract['transformation_name'],
                        "status": "error",
                        "error": str(e)
                    })
            
            successful = len([r for r in results if r['status'] == 'success'])
            
            return {
                "frequency": frequency,
                "contracts_executed": len(contracts),
                "contracts_successful": successful,
                "contracts_failed": len(contracts) - successful,
                "results": results,
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error executing scheduled contracts for {frequency}: {e}")
            return {
                "frequency": frequency,
                "status": "error",
                "error": str(e)
            }
    
    def should_execute_now(self, frequency: str, last_execution: Optional[datetime] = None) -> bool:
        """
        Determine if a contract should execute based on frequency and last execution time.
        
        Args:
            frequency: Execution frequency
            last_execution: Last execution timestamp (optional)
            
        Returns:
            bool: True if should execute now
        """
        if not last_execution:
            return True  # Never executed, should execute
        
        now = datetime.now()
        time_since_last = now - last_execution
        
        if frequency == "hourly":
            return time_since_last >= timedelta(hours=1)
        elif frequency == "daily":
            return time_since_last >= timedelta(days=1)
        elif frequency == "weekly":
            return time_since_last >= timedelta(weeks=1)
        elif frequency == "monthly":
            return time_since_last >= timedelta(days=30)
        else:
            return False

