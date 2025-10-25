# Bronze Schema Entity Relationship Diagram

## ERD for KIMBALL Discovery Results

This diagram shows the discovered relationships between tables in the bronze schema based on KIMBALL analysis.

```mermaid
erDiagram
    Master_Data_Repository {
        string ID PK
        string Description1
        string Vendor_Item_#
        string Location
        string Item_Class
        string Item_Category
        string Unit_of_Issue
        string Vendor_1_Name
    }
    
    ProductMapping {
        string Prod_Num PK
        string Service_Name PK
        string Product_Category
        string Unit_Qualify
        string Finishing_Material
        string Value_Stream
        string Business_Unit
        string ABS_Sales_Dept
    }
    
    DeptAllocation {
        string item_code PK
        string Crown_Bridge
        string Full_Arch
        string Removables
        string Other
    }
    
    Zimbis_Irvine {
        string Time PK
        string Tran_No PK
        string Date
        string Item
        string ItemDescr
        string Current_patient_ID
        string Current_patient_name
        string Current_Client_id
        string Current_Client_Name
        float Issue_Cost
        float ExtCost
        string Container_#
        string Lot_Num
    }
    
    Zimbis_West_Hills {
        string Time PK
        string Tran_No PK
        string Date
        string Item
        string ItemDescr
        string Current_patient_ID
        string Current_patient_name
        string Current_Client_id
        string Current_Client_Name
        float Issue_Cost
        float ExtCost
        string Container_#
        string Lot_Num
    }
    
    abs_sales {
        string item_description
        float current_year_sales_amount
        float previous_year_sales_amount
        float current_year_units_sold
        string productnumber
        string location_id
        string service_name
        string subcat_name
        string value_stream
        string bu_name
    }
    
    zimbis_consumption {
        string item_description
        float extended_cost
        int on_hand_qty
        float unit_cost
        string location_id
        string item_class
        string site
    }
    
    payroll_overhead {
        float debit
        float credit
        float extended_cost
        string location_id
        string labor_dpt
        string labor_type
        string gl_account
        string bu_name
    }
    
    cogs_direct_materials {
        float extended_cost
        string productnumber
        string location_id
        string item_class
        string value_stream
        string bu_name
        string subcat_name
    }
    
    locations {
        int id PK
        string location_id
    }
    
    YTD_Payroll {
        string DATE
        string DESC
        string LOCATION
        string NAME
        string DEPARTMENT
        string ACCOUNT
        string DEBIT
        string CREDIT
        string EENAME
    }
    
    LaborMapping {
        string engage_dpt
        string bu_name
        string labor_dpt
        string labor_type
        string gl_account
    }
    
    DirectMaterialsMapping {
        string Product
        string Analog
        string Abutment
        string Implant_Parts
        string Misc_Parts
        string Resin
        string Alloy
        string PMMA
        string Denture_Teeth
        string Porcelain
        string Lithium_Disilicate
        string Zirconia
        string Wax
    }
    
    cdc_audit {
        datetime event_time
        string operation
        string table_name
        string primary_key
        string old_values
        string new_values
        string user
        string source
        string batch_id
    }
    
    %% High-Confidence Relationships (Validated)
    Zimbis_Irvine ||--|| Zimbis_West_Hills : "Tran_No"
    Zimbis_Irvine ||--|| Zimbis_West_Hills : "Time"
    
    %% Location Relationships
    abs_sales ||--o{ locations : "location_id"
    zimbis_consumption ||--o{ locations : "location_id"
    payroll_overhead ||--o{ locations : "location_id"
    cogs_direct_materials ||--o{ locations : "location_id"
    
    %% Product Relationships (Potential)
    Master_Data_Repository ||--o{ ProductMapping : "ID"
    ProductMapping ||--o{ abs_sales : "Prod_Num"
    ProductMapping ||--o{ zimbis_consumption : "Service_Name"
    
    %% Department Relationships
    DeptAllocation ||--o{ LaborMapping : "item_code"
    LaborMapping ||--o{ payroll_overhead : "labor_dpt"
    
    %% Audit Relationships
    cdc_audit ||--o{ Master_Data_Repository : "table_name"
    cdc_audit ||--o{ ProductMapping : "table_name"
    cdc_audit ||--o{ Zimbis_Irvine : "table_name"
    cdc_audit ||--o{ Zimbis_West_Hills : "table_name"
```

## Relationship Summary

### Validated Relationships (High Confidence)
1. **Zimbis_Irvine ↔ Zimbis_West_Hills**: Same transaction system, different locations
   - Join on: `Tran_No` (81% confidence)
   - Join on: `Time` (81% confidence)

### Location-Based Relationships
- All fact tables connect to `locations` via `location_id`
- Enables geographic analysis and reporting

### Product-Based Relationships
- `Master_Data_Repository` → `ProductMapping` (master product data)
- Product dimensions connect to sales and consumption facts

### Department-Based Relationships
- `DeptAllocation` → `LaborMapping` (department allocations)
- Labor mapping connects to payroll overhead

### Audit Trail
- `cdc_audit` tracks changes across all major tables
- Provides data lineage and change tracking

## Key Insights

1. **Transaction System**: Zimbis_Irvine and Zimbis_West_Hills are the same system at different locations
2. **Location Hub**: The `locations` table is a central dimension for geographic analysis
3. **Product Hierarchy**: Master_Data_Repository → ProductMapping → Fact tables
4. **Department Structure**: DeptAllocation → LaborMapping → Payroll
5. **Audit Capability**: Full CDC tracking available for data governance

## Next Steps for Validation

1. **Confirm Join Logic**: Validate the Tran_No and Time relationships between Zimbis tables
2. **Location Mapping**: Verify location_id consistency across fact tables
3. **Product Hierarchy**: Confirm the product data flow from master to facts
4. **Department Structure**: Validate department allocation logic
5. **Audit Requirements**: Define CDC requirements for each table
