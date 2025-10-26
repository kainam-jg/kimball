"""
Universal Data Processing Framework

This module provides a unified approach to chunking and parallelizing data extraction
from ANY source type (storage, database, API) with optimal performance strategies.
"""

import asyncio
import concurrent.futures
from typing import Dict, List, Any, Optional, Callable, Generator, Tuple
from dataclasses import dataclass
from datetime import datetime
import math
import threading
from queue import Queue
import time

from ..core.logger import Logger

@dataclass
class ChunkConfig:
    """Configuration for chunking strategy."""
    max_chunk_size: int = 100000  # Records per chunk (increased from 50K)
    max_parallel_workers: int = 8  # Parallel processing threads
    temp_table_prefix: str = "temp_chunk_"  # Prefix for temporary tables
    merge_strategy: str = "union_all"  # How to merge chunks: union_all, insert_into
    batch_size: int = 10000  # Records per batch within chunk (increased from 1K)

@dataclass
class ProcessingResult:
    """Result of processing a single chunk."""
    chunk_id: int
    source_chunk: Any  # Original chunk data
    records_processed: int
    records_loaded: int
    target_table: str
    temp_table: Optional[str] = None
    status: str = "success"
    error: Optional[str] = None
    processing_time: float = 0.0

class UniversalDataProcessor:
    """
    Universal data processor that handles chunking and parallelization
    for ANY data source type with optimal performance strategies.
    """
    
    def __init__(self, config: ChunkConfig = None):
        self.config = config or ChunkConfig()
        self.logger = Logger("universal_processor")
        self._temp_tables = []  # Track temporary tables for cleanup
        
    async def process_large_dataset(
        self,
        data_source: Any,
        source_type: str,
        target_table: str,
        extractor_func: Callable,
        converter_func: Callable,
        loader_func: Callable
    ) -> Dict[str, Any]:
        """
        Process large datasets with universal chunking and parallelization.
        
        Args:
            data_source: The source object (S3 client, DB connector, etc.)
            source_type: Type of source (s3, postgres, api, etc.)
            target_table: Final target table name
            extractor_func: Function to extract data from source
            converter_func: Function to convert data to string streams
            loader_func: Function to load data into ClickHouse
            
        Returns:
            Processing summary with statistics
        """
        try:
            start_time = time.time()
            
            # Step 1: Determine dataset size and chunking strategy
            dataset_info = await self._analyze_dataset(data_source, source_type, extractor_func)
            
            # Step 2: Generate optimal chunking plan
            chunk_plan = self._create_chunk_plan(dataset_info, target_table)
            
            # Step 3: Process chunks in parallel
            results = await self._process_chunks_parallel(
                data_source, source_type, chunk_plan, 
                extractor_func, converter_func, loader_func
            )
            
            # Step 4: Merge temporary tables if needed
            if chunk_plan.use_temp_tables:
                await self._merge_temp_tables(results, target_table)
            
            # Step 5: Cleanup temporary resources
            await self._cleanup_temp_resources()
            
            # Calculate final statistics
            total_time = time.time() - start_time
            total_records = sum(r.records_loaded for r in results if r.status == "success")
            
            return {
                "status": "success",
                "total_records": total_records,
                "total_chunks": len(results),
                "successful_chunks": len([r for r in results if r.status == "success"]),
                "failed_chunks": len([r for r in results if r.status == "error"]),
                "processing_time": total_time,
                "records_per_second": total_records / total_time if total_time > 0 else 0,
                "chunk_strategy": chunk_plan.strategy,
                "parallel_workers": self.config.max_parallel_workers
            }
            
        except Exception as e:
            self.logger.error(f"Universal processing failed: {e}")
            await self._cleanup_temp_resources()
            raise
    
    async def _analyze_dataset(self, data_source: Any, source_type: str, extractor_func: Callable) -> Dict[str, Any]:
        """Analyze dataset to determine size and characteristics."""
        try:
            if source_type == "s3":
                # For S3, we need to analyze file size
                return await self._analyze_s3_dataset(data_source, extractor_func)
            elif source_type == "postgres":
                # For databases, we can get exact row counts
                return await self._analyze_database_dataset(data_source, extractor_func)
            elif source_type == "api":
                # For APIs, we might need to paginate
                return await self._analyze_api_dataset(data_source, extractor_func)
            else:
                # Default analysis
                return {"estimated_size": 100000, "chunking_strategy": "fixed_size"}
                
        except Exception as e:
            self.logger.error(f"Dataset analysis failed: {e}")
            return {"estimated_size": 100000, "chunking_strategy": "fixed_size"}
    
    async def _analyze_s3_dataset(self, s3_client, extractor_func: Callable) -> Dict[str, Any]:
        """Analyze S3 dataset characteristics."""
        try:
            # Get file size and estimate records
            # This would need to be implemented based on your S3 structure
            return {
                "estimated_size": 1000000,  # Estimate based on file size
                "chunking_strategy": "file_streaming",
                "supports_streaming": True
            }
        except Exception as e:
            self.logger.error(f"S3 analysis failed: {e}")
            return {"estimated_size": 100000, "chunking_strategy": "fixed_size"}
    
    async def _analyze_database_dataset(self, db_connector, extractor_func: Callable) -> Dict[str, Any]:
        """Analyze database dataset characteristics."""
        try:
            # Get exact row count
            count_query = "SELECT COUNT(*) as total_count FROM vehicles.daily_sales"
            result = db_connector.execute_query(count_query)
            total_count = result[0]['total_count'] if result else 0
            
            return {
                "estimated_size": total_count,
                "exact_size": total_count,
                "chunking_strategy": "offset_based",
                "supports_streaming": False
            }
        except Exception as e:
            self.logger.error(f"Database analysis failed: {e}")
            return {"estimated_size": 100000, "chunking_strategy": "fixed_size"}
    
    def _create_chunk_plan(self, dataset_info: Dict[str, Any], target_table: str) -> 'ChunkPlan':
        """Create optimal chunking plan based on dataset characteristics."""
        estimated_size = dataset_info.get("estimated_size", 100000)
        chunking_strategy = dataset_info.get("chunking_strategy", "fixed_size")
        
        # Determine if we need temporary tables
        use_temp_tables = estimated_size > 500000  # Use temp tables for > 500K records
        
        # Calculate optimal chunk size
        if estimated_size > 10000000:  # > 10M records
            chunk_size = 500000  # Much larger chunks for huge datasets
            max_workers = min(self.config.max_parallel_workers, 16)
        elif estimated_size > 1000000:  # > 1M records
            chunk_size = 200000  # Larger chunks for big datasets
            max_workers = min(self.config.max_parallel_workers, 12)
        elif estimated_size > 100000:  # > 100K records
            chunk_size = 100000  # Increased chunk size
            max_workers = min(self.config.max_parallel_workers, 8)
        else:
            chunk_size = 50000  # Increased even for smaller datasets
            max_workers = min(self.config.max_parallel_workers, 4)
        
        # Calculate number of chunks
        num_chunks = math.ceil(estimated_size / chunk_size)
        
        return ChunkPlan(
            total_records=estimated_size,
            chunk_size=chunk_size,
            num_chunks=num_chunks,
            max_workers=max_workers,
            strategy=chunking_strategy,
            use_temp_tables=use_temp_tables,
            target_table=target_table
        )
    
    async def _process_chunks_parallel(
        self,
        data_source: Any,
        source_type: str,
        chunk_plan: 'ChunkPlan',
        extractor_func: Callable,
        converter_func: Callable,
        loader_func: Callable
    ) -> List[ProcessingResult]:
        """Process chunks in parallel with optimal worker management."""
        
        # Create semaphore to limit concurrent workers
        semaphore = asyncio.Semaphore(chunk_plan.max_workers)
        
        # Create tasks for all chunks
        tasks = []
        for chunk_id in range(chunk_plan.num_chunks):
            task = self._process_single_chunk(
                semaphore, chunk_id, data_source, source_type, chunk_plan,
                extractor_func, converter_func, loader_func
            )
            tasks.append(task)
        
        # Execute all tasks in parallel
        self.logger.info(f"Processing {len(tasks)} chunks with {chunk_plan.max_workers} parallel workers")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(ProcessingResult(
                    chunk_id=i,
                    source_chunk=None,
                    records_processed=0,
                    records_loaded=0,
                    target_table=chunk_plan.target_table,
                    status="error",
                    error=str(result)
                ))
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _process_single_chunk(
        self,
        semaphore: asyncio.Semaphore,
        chunk_id: int,
        data_source: Any,
        source_type: str,
        chunk_plan: 'ChunkPlan',
        extractor_func: Callable,
        converter_func: Callable,
        loader_func: Callable
    ) -> ProcessingResult:
        """Process a single chunk with proper resource management."""
        
        async with semaphore:  # Limit concurrent workers
            try:
                start_time = time.time()
                
                # Determine target table (temp or final)
                if chunk_plan.use_temp_tables:
                    temp_table = f"{self.config.temp_table_prefix}{chunk_plan.target_table}_{chunk_id}"
                    self._temp_tables.append(temp_table)
                    target_table = temp_table
                else:
                    target_table = chunk_plan.target_table
                
                # Extract chunk data
                chunk_data = await self._extract_chunk_data(
                    data_source, source_type, chunk_plan, chunk_id, extractor_func
                )
                
                if not chunk_data:
                    return ProcessingResult(
                        chunk_id=chunk_id,
                        source_chunk=None,
                        records_processed=0,
                        records_loaded=0,
                        target_table=target_table,
                        status="success"  # Empty chunk is not an error
                    )
                
                # Convert to string streams
                string_data = converter_func(chunk_data)
                
                # Load into ClickHouse
                records_loaded = await loader_func(string_data, target_table)
                
                processing_time = time.time() - start_time
                
                self.logger.info(f"Chunk {chunk_id}: {len(chunk_data)} records processed, {records_loaded} loaded in {processing_time:.2f}s")
                
                return ProcessingResult(
                    chunk_id=chunk_id,
                    source_chunk=chunk_data,
                    records_processed=len(chunk_data),
                    records_loaded=records_loaded,
                    target_table=target_table,
                    temp_table=temp_table if chunk_plan.use_temp_tables else None,
                    status="success",
                    processing_time=processing_time
                )
                
            except Exception as e:
                self.logger.error(f"Chunk {chunk_id} processing failed: {e}")
                return ProcessingResult(
                    chunk_id=chunk_id,
                    source_chunk=None,
                    records_processed=0,
                    records_loaded=0,
                    target_table=chunk_plan.target_table,
                    status="error",
                    error=str(e)
                )
    
    async def _extract_chunk_data(
        self,
        data_source: Any,
        source_type: str,
        chunk_plan: 'ChunkPlan',
        chunk_id: int,
        extractor_func: Callable
    ) -> List[Dict[str, Any]]:
        """Extract data for a specific chunk based on source type."""
        
        if source_type == "postgres":
            # Database chunking with OFFSET/LIMIT
            offset = chunk_id * chunk_plan.chunk_size
            chunk_data = await self._extract_database_chunk(
                data_source, offset, chunk_plan.chunk_size, extractor_func
            )
        elif source_type == "s3":
            # S3 file streaming chunking
            chunk_data = await self._extract_s3_chunk(
                data_source, chunk_id, chunk_plan, extractor_func
            )
        else:
            # Generic chunking
            chunk_data = await extractor_func(data_source, chunk_id, chunk_plan.chunk_size)
        
        return chunk_data
    
    async def _extract_database_chunk(
        self,
        db_connector,
        offset: int,
        limit: int,
        extractor_func: Callable
    ) -> List[Dict[str, Any]]:
        """Extract database chunk using OFFSET/LIMIT."""
        try:
            chunk_query = f"SELECT * FROM vehicles.daily_sales LIMIT {limit} OFFSET {offset}"
            return db_connector.execute_query(chunk_query)
        except Exception as e:
            self.logger.error(f"Database chunk extraction failed: {e}")
            return []
    
    async def _extract_s3_chunk(
        self,
        s3_client,
        chunk_id: int,
        chunk_plan: 'ChunkPlan',
        extractor_func: Callable
    ) -> List[Dict[str, Any]]:
        """Extract S3 chunk using streaming approach."""
        try:
            # This would implement streaming chunking for large files
            # For now, return empty - needs implementation based on your S3 structure
            return []
        except Exception as e:
            self.logger.error(f"S3 chunk extraction failed: {e}")
            return []
    
    async def _merge_temp_tables(self, results: List[ProcessingResult], final_table: str):
        """Merge temporary tables into final table."""
        try:
            successful_temp_tables = [
                r.temp_table for r in results 
                if r.status == "success" and r.temp_table
            ]
            
            if not successful_temp_tables:
                return
            
            self.logger.info(f"Merging {len(successful_temp_tables)} temporary tables into {final_table}")
            
            # Create UNION ALL query to merge all temp tables
            union_queries = []
            for temp_table in successful_temp_tables:
                union_queries.append(f"SELECT * FROM bronze.{temp_table}")
            
            merge_query = f"""
            INSERT INTO bronze.{final_table}
            {chr(10).join(union_queries)}
            """
            
            # Execute merge query
            from ..core.database import DatabaseManager
            db_manager = DatabaseManager()
            db_manager.execute_query(merge_query)
            
            self.logger.info(f"Successfully merged {len(successful_temp_tables)} temp tables")
            
        except Exception as e:
            self.logger.error(f"Temp table merging failed: {e}")
            raise
    
    async def _cleanup_temp_resources(self):
        """Clean up temporary tables and resources."""
        try:
            if not self._temp_tables:
                return
            
            from ..core.database import DatabaseManager
            db_manager = DatabaseManager()
            
            for temp_table in self._temp_tables:
                try:
                    drop_query = f"DROP TABLE IF EXISTS bronze.{temp_table}"
                    db_manager.execute_query(drop_query)
                except Exception as e:
                    self.logger.warning(f"Failed to drop temp table {temp_table}: {e}")
            
            self.logger.info(f"Cleaned up {len(self._temp_tables)} temporary tables")
            self._temp_tables.clear()
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")

@dataclass
class ChunkPlan:
    """Plan for chunking a dataset."""
    total_records: int
    chunk_size: int
    num_chunks: int
    max_workers: int
    strategy: str
    use_temp_tables: bool
    target_table: str
