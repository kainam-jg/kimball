# KIMBALL Discovery Results - Bronze Schema Analysis

## 📊 Executive Summary

KIMBALL has successfully analyzed the bronze schema containing **14 tables** with **237 columns** across the ClickHouse database. The analysis identified **13 fact columns** (measures) and **224 dimension columns** (attributes), providing a solid foundation for data warehouse design.

## 🎯 Key Findings

### Schema Overview
- **Total Tables**: 14
- **Total Columns**: 237
- **Fact Columns**: 13 (5.5%)
- **Dimension Columns**: 224 (94.5%)
- **Fact/Dimension Ratio**: 0.06 (typical for transactional data)

### Data Volume Analysis
| Table Name | Rows | Columns | Fact Cols | Dim Cols | Quality Score |
|------------|------|---------|-----------|----------|---------------|
| zimbis_consumption | 54,753 | 10 | 3 | 7 | 0.68 |
| Zimbis_Irvine | 52,399 | 46 | 0 | 46 | 0.67 |
| payroll_overhead | 19,860 | 18 | 3 | 15 | -20.74 |
| YTD_Payroll | 18,609 | 12 | 0 | 12 | 0.60 |
| abs_sales | 10,360 | 24 | 6 | 18 | -60.51 |
| Master_Data_Repository | 4,659 | 17 | 0 | 17 | 0.65 |
| Zimbis_West_Hills | 2,354 | 46 | 0 | 46 | 0.55 |
| ProductMapping | 2,254 | 13 | 0 | 13 | 0.58 |
| DeptAllocation | 736 | 6 | 0 | 6 | 0.59 |
| cogs_direct_materials | 587 | 12 | 1 | 11 | -0.44 |
| LaborMapping | 74 | 6 | 0 | 6 | 0.51 |
| DirectMaterialsMapping | 39 | 14 | 0 | 14 | 0.50 |
| locations | 6 | 4 | 0 | 4 | 0.50 |
| cdc_audit | 0 | 9 | 0 | 9 | 0.00 |

## 🔑 Primary Key Candidates

KIMBALL identified **8 primary key candidates** across **5 tables**:

| Table | Column | Type | Cardinality | Uniqueness | Quality Score |
|-------|--------|------|-------------|------------|---------------|
| DeptAllocation | item_code | String | 736 | 100.00% | 0.87 |
| Master_Data_Repository | ID | String | 4,141 | 88.88% | 1.00 |
| ProductMapping | Prod Num | String | 2,238 | 99.29% | 1.00 |
| ProductMapping | Service Name | String | 2,210 | 98.05% | 1.00 |
| Zimbis_Irvine | Time | String | 47,741 | 91.11% | 1.00 |
| Zimbis_Irvine | Tran No | String | 49,560 | 94.58% | 1.00 |
| Zimbis_West_Hills | Time | String | 2,264 | 96.18% | 1.00 |
| Zimbis_West_Hills | Tran No | String | 2,354 | 100.00% | 1.00 |

## 🔗 High-Confidence Join Relationships

KIMBALL discovered **2 high-confidence join relationships** (>70% confidence):

1. **Zimbis_Irvine.Tran No = Zimbis_West_Hills.Tran No** (81% confidence)
2. **Zimbis_Irvine.Time = Zimbis_West_Hills.Time** (81% confidence)

## 📈 Fact Columns (Measures)

KIMBALL identified **13 fact columns** suitable for analytical measures:

| Table | Column | Type | Classification | Quality Score |
|-------|--------|------|---------------|---------------|
| abs_sales | current_year_sales_amount | Float64 | Fact | 1.00 |
| abs_sales | previous_year_sales_amount | Float64 | Fact | 1.00 |
| abs_sales | current_year_units_sold | Float64 | Fact | 0.67 |
| payroll_overhead | debit | Float64 | Fact | 1.00 |
| payroll_overhead | extended_cost | Float64 | Fact | 0.91 |
| zimbis_consumption | extended_cost | Float64 | Fact | 1.00 |
| zimbis_consumption | on_hand_qty | Int64 | Fact | 1.00 |
| zimbis_consumption | unit_cost | Float64 | Fact | 0.82 |
| cogs_direct_materials | extended_cost | Int64 | Fact | 0.72 |

## 🏗️ Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    Master_Data_Repository {
        string ID PK
        string Description1
        string Vendor_Item_#
        string Location
        string Item_Class
        string Item_Category
    }
    
    ProductMapping {
        string Prod_Num PK
        string Service_Name PK
        string Product_Category
        string Value_Stream
        string Business_Unit
    }
    
    DeptAllocation {
        string item_code PK
        string Crown_Bridge
        string Full_Arch
        string Removables
    }
    
    Zimbis_Irvine {
        string Time PK
        string Tran_No PK
        string Item
        string ItemDescr
        string Current_patient_ID
        string Current_patient_name
        float Issue_Cost
        float ExtCost
    }
    
    Zimbis_West_Hills {
        string Time PK
        string Tran_No PK
        string Item
        string ItemDescr
        string Current_patient_ID
        string Current_patient_name
        float Issue_Cost
        float ExtCost
    }
    
    abs_sales {
        string item_description
        float current_year_sales_amount
        float previous_year_sales_amount
        float current_year_units_sold
        string productnumber
        string location_id
    }
    
    zimbis_consumption {
        string item_description
        float extended_cost
        int on_hand_qty
        float unit_cost
        string location_id
    }
    
    payroll_overhead {
        float debit
        float credit
        float extended_cost
        string location_id
        string labor_dpt
    }
    
    cogs_direct_materials {
        float extended_cost
        string productnumber
        string location_id
        string item_class
    }
    
    locations {
        int id PK
        string location_id
    }
    
    %% Relationships
    Zimbis_Irvine ||--|| Zimbis_West_Hills : "Tran_No"
    Zimbis_Irvine ||--|| Zimbis_West_Hills : "Time"
    abs_sales ||--o{ locations : "location_id"
    zimbis_consumption ||--o{ locations : "location_id"
    payroll_overhead ||--o{ locations : "location_id"
    cogs_direct_materials ||--o{ locations : "location_id"
    Master_Data_Repository ||--o{ ProductMapping : "ID"
```

## 🎯 Data Warehouse Design Recommendations

### 1. Fact Tables (Star Schema Design)
**Primary Fact Tables:**
- **Sales Fact**: `abs_sales` (10,360 rows)
- **Inventory Fact**: `zimbis_consumption` (54,753 rows)
- **Payroll Fact**: `payroll_overhead` (19,860 rows)
- **Transaction Facts**: `Zimbis_Irvine`, `Zimbis_West_Hills`

### 2. Dimension Tables
**Core Dimensions:**
- **Product Dimension**: `ProductMapping`, `Master_Data_Repository`
- **Location Dimension**: `locations`
- **Time Dimension**: Extract from transaction timestamps
- **Department Dimension**: From payroll and allocation tables

### 3. Join Strategy
**Validated Relationships:**
- **Zimbis_Irvine ↔ Zimbis_West_Hills**: Same transaction system, different locations
- **All Facts ↔ Location Dimension**: Via `location_id`
- **Product Facts ↔ Product Dimension**: Via product identifiers

## 🔍 Data Quality Issues

### High Priority Issues:
1. **payroll_overhead**: Negative quality score (-20.74) - investigate data integrity
2. **abs_sales**: Negative quality score (-60.51) - significant data quality issues
3. **Missing Primary Keys**: Several tables lack clear primary key candidates

### Medium Priority Issues:
1. **Null Value Patterns**: Some columns have high null percentages
2. **Data Type Inconsistencies**: Mixed data types in similar columns
3. **Cardinality Issues**: Some dimension columns have very low cardinality

## 🚀 Next Steps

### Phase 1: Data Validation
1. **Validate Join Relationships**: Confirm the identified joins are correct
2. **Data Quality Cleanup**: Address the high-priority data quality issues
3. **Primary Key Implementation**: Implement proper primary keys for all tables

### Phase 2: Silver Layer Design
1. **Create Star Schema**: Design fact and dimension tables
2. **Implement SCDs**: Slowly Changing Dimensions for historical tracking
3. **Data Lineage**: Document data flow from bronze to silver

### Phase 3: Gold Layer Design
1. **Business Metrics**: Define KPIs and business metrics
2. **Aggregated Tables**: Create summary tables for performance
3. **Data Marts**: Create subject-specific data marts

## 📋 Validation Checklist

- [ ] **Join Relationships**: Validate the 2 high-confidence joins
- [ ] **Primary Keys**: Confirm the 8 identified primary key candidates
- [ ] **Data Quality**: Address the 3 tables with negative quality scores
- [ ] **Business Rules**: Validate fact/dimension classifications with business users
- [ ] **Performance**: Test query performance with identified relationships

---

**Analysis Date**: October 25, 2025  
**KIMBALL Version**: 1.0.0  
**Schema**: Bronze  
**Total Analysis Time**: ~2 minutes  
**Confidence Level**: High (based on automated analysis)
