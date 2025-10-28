import pandas as pd
import numpy as np
from datetime import datetime
import holidays
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def generate_date_dataframe(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Generates a dataframe containing date attributes between start_date and end_date.

    Parameters:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: A dataframe containing date attributes.
    """
    logger.info(f"Generating calendar dimension from {start_date} to {end_date}")
    
    # Generate a range of dates
    dates = pd.date_range(start=start_date, end=end_date)

    # Create dataframe with date attributes
    df = pd.DataFrame({"calendar_date": dates})
    df["calendar_id"] = df["calendar_date"].dt.strftime('%Y%m%d').astype(int)
    df["calendar_year"] = df["calendar_date"].dt.year
    df["calendar_year_qtr"] = df["calendar_date"].dt.to_period("Q").astype(str).str[0:4] + '-' + df["calendar_date"].dt.to_period("Q").astype(str).str[4:6]
    df["calendar_qtr_num"] = df["calendar_date"].dt.quarter
    df["calendar_qtr_name"] = df["calendar_date"].dt.to_period("Q").astype(str).str[4:6]
    df["calendar_year_month"] = df["calendar_date"].dt.to_period("M").astype(str)
    df["calendar_month_num"] = df["calendar_date"].dt.month
    df["calendar_month_name"] = df["calendar_date"].dt.month_name()
    df["calendar_year_week"] = df["calendar_date"].dt.strftime("%Y-%W")
    df["calendar_week_num"] = df["calendar_year_week"].str[5:7].astype(int).astype(str)
    df["calendar_week_name"] = "Wk"+df["calendar_week_num"].astype(str)
    df["calendar_day"] = df["calendar_date"].dt.weekday + 1  # Monday = 1, Sunday = 7
    df["calendar_day_name"] = df["calendar_date"].dt.day_name()
    df["is_weekend"] = np.where(df["calendar_day"] >= 6, 1, 0)
    df["day_of_month"] = df["calendar_date"].dt.day
    df["days_in_month"] = df["calendar_date"].dt.daysinmonth

    # US Holidays
    us_holiday_map = holidays.US(years=range(df["calendar_year"].min(), df["calendar_year"].max() + 1))
    df["holiday_name"] = df["calendar_date"].map(us_holiday_map).fillna("")
    df["holiday_flag"] = np.where(df["holiday_name"] != "", "Y", "N")

    # Working day flag
    df["working"] = np.where((df["calendar_day"].between(1, 5)) & (df["holiday_flag"] == "N"), 1, 0)

    # Add cumulative working days within each month
    df["working_day"] = df.groupby("calendar_year_month")["working"].cumsum()
    df["working_days"] = df.groupby("calendar_year_month")["working"].transform("sum")

    logger.info(f"Generated {len(df)} calendar records")
    return df


class CalendarGenerator:
    """
    Calendar dimension generator for the Model phase.
    Creates calendar_dim table in the gold schema.
    """
    
    def __init__(self, db_manager):
        """
        Initialize the CalendarGenerator.
        
        Args:
            db_manager: DatabaseManager instance for database operations
        """
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    def generate_calendar_dimension(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Generate calendar dimension table in silver schema.
        
        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format
            end_date (str): End date in 'YYYY-MM-DD' format
            
        Returns:
            Dict containing generation results and statistics
        """
        try:
            self.logger.info(f"Starting calendar dimension generation from {start_date} to {end_date}")
            
            # Generate the calendar dataframe
            df = generate_date_dataframe(start_date, end_date)
            
            # Create the table in gold schema
            self._create_calendar_table()
            
            # Insert the data
            self._insert_calendar_data(df)
            
            # Get final statistics
            stats = self._get_calendar_stats()
            
            result = {
                "status": "success",
                "message": f"Calendar dimension generated successfully",
                "start_date": start_date,
                "end_date": end_date,
                "total_records": len(df),
                "table_name": "silver.calendar_stage1",
                "statistics": stats
            }
            
            self.logger.info(f"Calendar dimension generation completed: {len(df)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error generating calendar dimension: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to generate calendar dimension: {str(e)}",
                "start_date": start_date,
                "end_date": end_date
            }
    
    def _create_calendar_table(self):
        """Create the calendar_stage1 table in silver schema."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver.calendar_stage1 (
            calendar_id UInt32,
            calendar_date Date,
            calendar_year UInt16,
            calendar_year_qtr String,
            calendar_qtr_num UInt8,
            calendar_qtr_name String,
            calendar_year_month String,
            calendar_month_num UInt8,
            calendar_month_name String,
            calendar_year_week String,
            calendar_week_num String,
            calendar_week_name String,
            calendar_day UInt8,
            calendar_day_name String,
            is_weekend UInt8,
            day_of_month UInt8,
            days_in_month UInt8,
            holiday_name String,
            holiday_flag String,
            working UInt8,
            working_day UInt8,
            working_days UInt8
        ) ENGINE = MergeTree()
        ORDER BY calendar_id
        """
        
        self.logger.info("Creating silver.calendar_stage1 table")
        success = self.db_manager.execute_command(create_table_sql)
        if not success:
            raise Exception("Failed to create calendar_stage1 table")
    
    def _insert_calendar_data(self, df: pd.DataFrame):
        """Insert calendar data into the table."""
        self.logger.info(f"Inserting {len(df)} calendar records")
        
        # Insert data row by row to avoid complex SQL construction
        for _, row in df.iterrows():
            # Escape single quotes in string values
            def escape_string(value):
                return str(value).replace("'", "''")
            
            insert_sql = f"""
            INSERT INTO silver.calendar_stage1 (
                calendar_id, calendar_date, calendar_year, calendar_year_qtr,
                calendar_qtr_num, calendar_qtr_name, calendar_year_month,
                calendar_month_num, calendar_month_name, calendar_year_week,
                calendar_week_num, calendar_week_name, calendar_day,
                calendar_day_name, is_weekend, day_of_month, days_in_month,
                holiday_name, holiday_flag, working, working_day, working_days
            ) VALUES (
                {int(row['calendar_id'])},
                '{row['calendar_date'].strftime('%Y-%m-%d')}',
                {int(row['calendar_year'])},
                '{escape_string(row['calendar_year_qtr'])}',
                {int(row['calendar_qtr_num'])},
                '{escape_string(row['calendar_qtr_name'])}',
                '{escape_string(row['calendar_year_month'])}',
                {int(row['calendar_month_num'])},
                '{escape_string(row['calendar_month_name'])}',
                '{escape_string(row['calendar_year_week'])}',
                '{escape_string(row['calendar_week_num'])}',
                '{escape_string(row['calendar_week_name'])}',
                {int(row['calendar_day'])},
                '{escape_string(row['calendar_day_name'])}',
                {int(row['is_weekend'])},
                {int(row['day_of_month'])},
                {int(row['days_in_month'])},
                '{escape_string(row['holiday_name'])}',
                '{escape_string(row['holiday_flag'])}',
                {int(row['working'])},
                {int(row['working_day'])},
                {int(row['working_days'])}
            )
            """
            
            success = self.db_manager.execute_command(insert_sql)
            if not success:
                raise Exception(f"Failed to insert calendar record for date {row['calendar_date']}")
    
    def _get_calendar_stats(self) -> Dict[str, Any]:
        """Get statistics about the generated calendar dimension."""
        try:
            # Get total count
            count_query = "SELECT COUNT(*) as total_records FROM silver.calendar_stage1"
            result = self.db_manager.execute_query_dict(count_query)
            total_records = result[0]['total_records'] if result else 0
            
            # Get date range
            range_query = """
            SELECT 
                MIN(calendar_date) as min_date,
                MAX(calendar_date) as max_date
            FROM silver.calendar_stage1
            """
            result = self.db_manager.execute_query_dict(range_query)
            min_date = result[0]['min_date'] if result else None
            max_date = result[0]['max_date'] if result else None
            
            return {
                "total_records": total_records,
                "date_range": {
                    "min_date": str(min_date) if min_date else None,
                    "max_date": str(max_date) if max_date else None
                }
            }
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve calendar statistics: {e}")
            return {"error": str(e)}


if __name__ == "__main__":
    # Define date range
    start_date = "1980-01-01"
    end_date = "2080-12-31"

    # Generate dataframe
    df = generate_date_dataframe(start_date, end_date)
    print(f"Generated {len(df)} calendar records")
    print(df.head())
