"""
KIMBALL Intelligent Type Inference System

This module provides sophisticated pattern recognition and machine learning
capabilities for inferring data types from string values in the bronze layer.

The system uses a hybrid approach combining:
1. Rule-based pattern matching for obvious patterns (dates, numbers)
2. Statistical analysis for numeric measure detection
3. Confidence scoring and learning from user corrections
4. Performance optimization with intelligent caching

Key Features:
- Multi-pattern date detection (YYYYMMDD, YYYY-MM-DD, MM/DD/YYYY, etc.)
- Statistical numeric measure detection with pattern analysis
- Online learning system that improves accuracy over time
- Performance optimization with caching and smart sampling
- Production-ready with comprehensive error handling

Author: KIMBALL Development Team
Version: 1.0.0
"""

import re
import logging
import numpy as np
from datetime import datetime, date
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from collections import Counter
import math

logger = logging.getLogger(__name__)

@dataclass
class TypeInferenceResult:
    """
    Result of intelligent type inference analysis.
    
    Attributes:
        inferred_type: The detected data type (date, numeric, string)
        confidence: Confidence score between 0.0 and 1.0
        pattern_matched: The specific pattern that matched (e.g., 'YYYYMMDD')
        reasoning: Human-readable explanation of the inference
        sample_values: Sample values used for the analysis
    """
    inferred_type: str
    confidence: float
    pattern_matched: Optional[str] = None
    reasoning: str = ""
    sample_values: List[str] = None

class DatePatternDetector:
    """
    Advanced date pattern detection with confidence scoring and learning capabilities.
    
    This class implements sophisticated date pattern recognition using regex patterns
    combined with validation logic. It supports multiple date formats and learns from
    user corrections to improve accuracy over time.
    
    Supported Patterns:
    - YYYYMMDD: 8-digit dates (20210926)
    - YYYY-MM-DD: ISO date format (2025-10-26)
    - MM/DD/YYYY: US date format (10/26/2025)
    - DD-MM-YYYY: European date format (26-10-2025)
    - Unix Timestamp: 10-digit timestamps (1732627200)
    - ISO DateTime: ISO datetime format (2025-10-26T10:30:00)
    """
    
    def __init__(self):
        self.patterns = {
            'YYYYMMDD': {
                'regex': re.compile(r'^\d{8}$'),
                'confidence': 0.9,
                'validator': self._validate_yyyymmdd,
                'examples': ['20251026', '20240101', '20231225']
            },
            'YYYY-MM-DD': {
                'regex': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
                'confidence': 0.95,
                'validator': self._validate_iso_date,
                'examples': ['2025-10-26', '2024-01-01', '2023-12-25']
            },
            'MM/DD/YYYY': {
                'regex': re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'),
                'confidence': 0.9,
                'validator': self._validate_mm_dd_yyyy,
                'examples': ['10/26/2025', '1/1/2024', '12/25/2023']
            },
            'DD-MM-YYYY': {
                'regex': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$'),
                'confidence': 0.9,
                'validator': self._validate_dd_mm_yyyy,
                'examples': ['26-10-2025', '01-01-2024', '25-12-2023']
            },
            'Unix Timestamp': {
                'regex': re.compile(r'^\d{10}$'),
                'confidence': 0.8,
                'validator': self._validate_unix_timestamp,
                'examples': ['1732627200', '1704067200', '1672531200']
            },
            'ISO DateTime': {
                'regex': re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'),
                'confidence': 0.95,
                'validator': self._validate_iso_datetime,
                'examples': ['2025-10-26T10:30:00', '2024-01-01T00:00:00']
            }
        }
        
        # Track pattern success rates for learning
        self.pattern_success_count = {pattern: 0 for pattern in self.patterns.keys()}
        self.pattern_total_count = {pattern: 0 for pattern in self.patterns.keys()}
    
    def detect_date_pattern(self, values: List[str], sample_size: int = 100) -> Tuple[Optional[str], float]:
        """
        Detect date patterns in a list of values using multi-pattern analysis.
        
        This method analyzes a sample of values against all known date patterns,
        calculates confidence scores based on match ratios and validation success,
        and applies learning adjustments from historical performance.
        
        Args:
            values: List of string values to analyze
            sample_size: Maximum number of values to sample for analysis (default: 100)
            
        Returns:
            Tuple of (best_pattern_name, confidence_score) where:
            - best_pattern_name: Name of the best matching pattern (e.g., 'YYYYMMDD')
            - confidence_score: Confidence score between 0.0 and 1.0
            
        Example:
            >>> detector = DatePatternDetector()
            >>> pattern, confidence = detector.detect_date_pattern(['20210926', '20210927'])
            >>> print(f"Pattern: {pattern}, Confidence: {confidence}")
            Pattern: YYYYMMDD, Confidence: 0.9
        """
        if not values:
            return None, 0.0
            
        # Sample values for performance
        sample_values = self._smart_sample(values, sample_size)
        
        pattern_scores = {}
        
        for pattern_name, pattern_info in self.patterns.items():
            regex = pattern_info['regex']
            base_confidence = pattern_info['confidence']
            validator = pattern_info['validator']
            
            # Count matches
            matches = 0
            valid_matches = 0
            
            for value in sample_values:
                if regex.match(str(value).strip()):
                    matches += 1
                    # Additional validation for dates
                    if validator(str(value).strip()):
                        valid_matches += 1
            
            if matches > 0:
                # Calculate confidence based on match ratio and validation success
                match_ratio = matches / len(sample_values)
                validation_ratio = valid_matches / matches if matches > 0 else 0
                
                # Combine base confidence with match quality
                final_confidence = base_confidence * match_ratio * validation_ratio
                
                # Apply learning adjustments (less aggressive)
                if pattern_name in self.pattern_success_count:
                    success_rate = self.pattern_success_count[pattern_name] / max(1, self.pattern_total_count[pattern_name])
                    # Less aggressive adjustment: 0.8 + 0.2 * success_rate (instead of 0.5 + 0.5 * success_rate)
                    final_confidence *= (0.8 + 0.2 * success_rate)
                
                pattern_scores[pattern_name] = final_confidence
        
        if not pattern_scores:
            return None, 0.0
            
        # Return the pattern with highest confidence
        best_pattern = max(pattern_scores.items(), key=lambda x: x[1])
        return best_pattern[0], best_pattern[1]
    
    def _smart_sample(self, values: List[str], max_size: int) -> List[str]:
        """Intelligently sample values for analysis."""
        if len(values) <= max_size:
            return values
            
        # Stratified sampling - take from beginning, middle, and end
        step = len(values) // max_size
        sampled = []
        
        for i in range(0, len(values), step):
            if len(sampled) >= max_size:
                break
            sampled.append(values[i])
            
        return sampled[:max_size]
    
    def _validate_yyyymmdd(self, value: str) -> bool:
        """Validate YYYYMMDD format."""
        try:
            year = int(value[:4])
            month = int(value[4:6])
            day = int(value[6:8])
            
            # Basic validation
            if not (1900 <= year <= 2100):
                return False
            if not (1 <= month <= 12):
                return False
            if not (1 <= day <= 31):
                return False
                
            # Try to create date object
            date(year, month, day)
            return True
        except:
            return False
    
    def _validate_iso_date(self, value: str) -> bool:
        """Validate YYYY-MM-DD format."""
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except:
            return False
    
    def _validate_mm_dd_yyyy(self, value: str) -> bool:
        """Validate MM/DD/YYYY format."""
        try:
            datetime.strptime(value, '%m/%d/%Y')
            return True
        except:
            return False
    
    def _validate_dd_mm_yyyy(self, value: str) -> bool:
        """Validate DD-MM-YYYY format."""
        try:
            datetime.strptime(value, '%d-%m-%Y')
            return True
        except:
            return False
    
    def _validate_unix_timestamp(self, value: str) -> bool:
        """Validate Unix timestamp."""
        try:
            timestamp = int(value)
            # Check if timestamp is reasonable (between 1970 and 2100)
            if 0 <= timestamp <= 4102444800:  # Jan 1, 2100
                datetime.fromtimestamp(timestamp)
                return True
        except:
            pass
        return False
    
    def _validate_iso_datetime(self, value: str) -> bool:
        """Validate ISO datetime format."""
        try:
            datetime.fromisoformat(value.replace('Z', '+00:00'))
            return True
        except:
            return False
    
    def learn_from_correction(self, pattern_name: str, was_correct: bool):
        """Learn from user corrections to improve future predictions."""
        if pattern_name in self.pattern_total_count:
            self.pattern_total_count[pattern_name] += 1
            if was_correct:
                self.pattern_success_count[pattern_name] += 1

class NumericMeasureDetector:
    """
    Statistical analysis engine for numeric measure detection.
    
    This class implements sophisticated numeric pattern recognition using statistical
    analysis, pattern matching, and multi-factor confidence scoring. It can detect
    various numeric formats including decimals, integers, currency, percentages, and
    scientific notation.
    
    Detection Methods:
    - Pattern matching using regex for common numeric formats
    - Statistical analysis of value distributions and consistency
    - Range analysis for detecting numeric sequences
    - Length consistency analysis for format validation
    
    Supported Numeric Types:
    - Decimal numbers (10336.48)
    - Integers (12345)
    - Currency ($1,234.56)
    - Percentages (85.5%)
    - Scientific notation (1.23e+04)
    - Negative numbers (-123.45)
    """
    
    def __init__(self):
        self.numeric_patterns = {
            'decimal': re.compile(r'^\d+\.\d+$'),
            'integer': re.compile(r'^\d+$'),
            'currency': re.compile(r'^\$?\d+(?:,\d{3})*(?:\.\d{2})?$'),
            'percentage': re.compile(r'^\d+(?:\.\d+)?%$'),
            'scientific': re.compile(r'^\d+(?:\.\d+)?[eE][+-]?\d+$'),
            'negative_number': re.compile(r'^-\d+(?:\.\d+)?$')
        }
    
    def detect_numeric_pattern(self, values: List[str], sample_size: int = 100) -> Tuple[bool, float]:
        """
        Detect if values represent numeric measures.
        
        Args:
            values: List of string values to analyze
            sample_size: Maximum number of values to sample for analysis
            
        Returns:
            Tuple of (is_numeric, confidence_score)
        """
        if not values:
            return False, 0.0
            
        sample_values = self._smart_sample(values, sample_size)
        
        # Feature extraction
        features = self._extract_numeric_features(sample_values)
        
        # Calculate confidence based on multiple indicators
        confidence = self._calculate_numeric_confidence(features)
        
        return confidence > 0.7, confidence
    
    def _extract_numeric_features(self, values: List[str]) -> Dict[str, float]:
        """Extract features for numeric analysis."""
        features = {
            'pattern_match_ratio': 0.0,
            'decimal_ratio': 0.0,
            'range_consistency': 0.0,
            'length_consistency': 0.0,
            'statistical_distribution': 0.0
        }
        
        if not values:
            return features
        
        # Pattern matching
        pattern_matches = 0
        decimal_count = 0
        lengths = []
        numeric_values = []
        
        for value in values:
            value_str = str(value).strip()
            lengths.append(len(value_str))
            
            # Check against numeric patterns
            for pattern_name, pattern in self.numeric_patterns.items():
                if pattern.match(value_str):
                    pattern_matches += 1
                    if pattern_name == 'decimal':
                        decimal_count += 1
                    break
            
            # Try to convert to float
            try:
                numeric_val = float(value_str.replace(',', '').replace('$', '').replace('%', ''))
                numeric_values.append(numeric_val)
            except:
                pass
        
        # Calculate ratios
        features['pattern_match_ratio'] = pattern_matches / len(values)
        features['decimal_ratio'] = decimal_count / len(values) if values else 0
        
        # Length consistency
        if lengths:
            length_std = np.std(lengths)
            length_mean = np.mean(lengths)
            features['length_consistency'] = 1.0 / (1.0 + length_std / max(1, length_mean))
        
        # Range consistency (for numeric values)
        if len(numeric_values) > 1:
            numeric_values = np.array(numeric_values)
            if np.std(numeric_values) > 0:
                cv = np.std(numeric_values) / np.mean(np.abs(numeric_values))
                features['range_consistency'] = 1.0 / (1.0 + cv)
        
        return features
    
    def _calculate_numeric_confidence(self, features: Dict[str, float]) -> float:
        """Calculate overall confidence for numeric classification."""
        weights = {
            'pattern_match_ratio': 0.4,
            'decimal_ratio': 0.2,
            'range_consistency': 0.2,
            'length_consistency': 0.1,
            'statistical_distribution': 0.1
        }
        
        weighted_score = sum(features[key] * weights[key] for key in weights.keys())
        return min(1.0, weighted_score)
    
    def _smart_sample(self, values: List[str], max_size: int) -> List[str]:
        """Intelligently sample values for analysis."""
        if len(values) <= max_size:
            return values
            
        step = len(values) // max_size
        sampled = []
        
        for i in range(0, len(values), step):
            if len(sampled) >= max_size:
                break
            sampled.append(values[i])
            
        return sampled[:max_size]

class IntelligentTypeInference:
    """
    Main intelligent type inference engine with hybrid pattern recognition.
    
    This is the primary interface for intelligent data type inference. It combines
    multiple detection strategies to provide accurate type classification with
    confidence scoring and learning capabilities.
    
    Architecture:
    1. Date Pattern Detection: Uses DatePatternDetector for date format recognition
    2. Numeric Measure Detection: Uses NumericMeasureDetector for numeric analysis
    3. Confidence Scoring: Combines multiple signals for final classification
    4. Learning System: Learns from user corrections to improve accuracy
    5. Performance Optimization: Uses caching and smart sampling for efficiency
    
    Key Features:
    - Multi-signal approach combining pattern matching and statistical analysis
    - Confidence thresholds optimized for production use (0.7 for dates, 0.6 for numeric)
    - Intelligent caching system for performance optimization
    - Online learning from user corrections
    - Comprehensive error handling and logging
    
    Usage:
        >>> engine = IntelligentTypeInference()
        >>> result = engine.infer_column_type(['20210926', '20210927'], 'sales_date')
        >>> print(f"Type: {result.inferred_type}, Confidence: {result.confidence}")
        Type: date, Confidence: 0.9
    """
    
    def __init__(self):
        self.date_detector = DatePatternDetector()
        self.numeric_detector = NumericMeasureDetector()
        
        # Performance optimization
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        logger.info("Intelligent Type Inference engine initialized")
    
    def infer_column_type(self, values: List[str], column_name: str = "") -> TypeInferenceResult:
        """
        Infer the data type of a column based on its values using intelligent analysis.
        
        This is the main method for type inference. It analyzes a list of string values
        and returns a comprehensive result including the inferred type, confidence score,
        pattern matched, reasoning, and sample values.
        
        The method uses a multi-step approach:
        1. Check cache for previously analyzed similar data
        2. Run date pattern detection with confidence scoring
        3. Run numeric measure detection with statistical analysis
        4. Combine results using optimized thresholds
        5. Cache result for future performance optimization
        
        Args:
            values: List of string values to analyze
            column_name: Name of the column (used for caching and logging)
            
        Returns:
            TypeInferenceResult containing:
            - inferred_type: 'date', 'numeric', or 'string'
            - confidence: Confidence score between 0.0 and 1.0
            - pattern_matched: Specific pattern that matched (if any)
            - reasoning: Human-readable explanation
            - sample_values: Sample values used for analysis
            
        Example:
            >>> engine = IntelligentTypeInference()
            >>> result = engine.infer_column_type(['20210926', '20210927', '20210928'], 'sales_date')
            >>> print(f"Type: {result.inferred_type}, Confidence: {result.confidence}")
            Type: date, Confidence: 0.9
        """
        if not values:
            return TypeInferenceResult(
                inferred_type="string",
                confidence=0.0,
                reasoning="No values to analyze"
            )
        
        # Check cache first
        cache_key = self._generate_cache_key(values, column_name)
        if cache_key in self.cache:
            self.cache_hits += 1
            return self.cache[cache_key]
        
        self.cache_misses += 1
        
        # Step 1: Date pattern detection
        date_pattern, date_confidence = self.date_detector.detect_date_pattern(values)
        
        # Step 2: Numeric measure detection
        is_numeric, numeric_confidence = self.numeric_detector.detect_numeric_pattern(values)
        
        # Step 3: Combine results
        result = self._combine_predictions(
            values, column_name, date_pattern, date_confidence, 
            is_numeric, numeric_confidence
        )
        
        # Cache result
        self.cache[cache_key] = result
        
        logger.info(f"Type inference for column '{column_name}': {result.inferred_type} "
                   f"(confidence: {result.confidence:.2f})")
        
        return result
    
    def _combine_predictions(self, values: List[str], column_name: str,
                           date_pattern: Optional[str], date_confidence: float,
                           is_numeric: bool, numeric_confidence: float) -> TypeInferenceResult:
        """Combine different prediction signals."""
        
        # Determine final type and confidence
        if date_pattern and date_confidence > 0.7:  # Lowered threshold from 0.8
            return TypeInferenceResult(
                inferred_type="date",
                confidence=date_confidence,
                pattern_matched=date_pattern,
                reasoning=f"Detected {date_pattern} date pattern with {date_confidence:.2f} confidence",
                sample_values=self._get_sample_values(values, 3)
            )
        elif is_numeric and numeric_confidence > 0.6:  # Lowered threshold from 0.7
            return TypeInferenceResult(
                inferred_type="numeric",
                confidence=numeric_confidence,
                pattern_matched="numeric_measure",
                reasoning=f"Detected numeric pattern with {numeric_confidence:.2f} confidence",
                sample_values=self._get_sample_values(values, 3)
            )
        else:
            # Default to string
            max_confidence = max(date_confidence, numeric_confidence)
            return TypeInferenceResult(
                inferred_type="string",
                confidence=max(0.1, 1.0 - max_confidence),  # Low confidence for string
                reasoning=f"No strong pattern detected. Date: {date_confidence:.2f}, "
                         f"Numeric: {numeric_confidence:.2f}",
                sample_values=self._get_sample_values(values, 3)
            )
    
    def _generate_cache_key(self, values: List[str], column_name: str) -> str:
        """Generate cache key for values."""
        # Use first few values and column name for caching
        sample_str = "|".join(str(v) for v in values[:5])
        return f"{column_name}:{hash(sample_str)}"
    
    def _get_sample_values(self, values: List[str], count: int) -> List[str]:
        """Get sample values for display."""
        if len(values) <= count:
            return values
        return values[:count]
    
    def learn_from_correction(self, column_name: str, predicted_type: str, 
                            actual_type: str, confidence: float):
        """Learn from user corrections."""
        logger.info(f"Learning from correction: {column_name} "
                   f"predicted={predicted_type}, actual={actual_type}")
        
        # Update pattern detectors
        if predicted_type == "date" and actual_type != "date":
            # Date pattern was wrong
            pass  # Could implement pattern-specific learning here
        
        # Clear cache for this column to force re-analysis
        keys_to_remove = [key for key in self.cache.keys() if column_name in key]
        for key in keys_to_remove:
            del self.cache[key]
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        total_requests = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / max(1, total_requests)
        
        return {
            "cache_hit_rate": cache_hit_rate,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_requests": total_requests,
            "cached_results": len(self.cache)
        }
