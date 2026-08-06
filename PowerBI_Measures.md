# Power BI Measures for Sales and Promotion Reporting

This document lists Power BI measures with example DAX formulas based on the available sales and promotion files in the workspace.

## Assumed table names
- `Sales` = `silver_DA_23_07_2026.sales.silver_orders` or `sales_clean.orders_clean`
- `PromotionHistory` = `Files/silver/scd2_seed_promotions`
- `SalesHistory` = `Files/silver/scd2_seed_sales`
- `ETLControl` = `sales_clean.ETLControl_01`
- `Date` = date dimension table on `order_date`

## Core sales measures

| Measure | Description | DAX |
|---|---|---|
| Total Sales | Sum of sales amount | `Total Sales = SUM(Sales[total_amount])` |
| Total Quantity | Sum of quantity sold | `Total Quantity = SUM(Sales[quantity])` |
| Total Orders | Count of unique orders | `Total Orders = DISTINCTCOUNT(Sales[order_id])` |
| Total Customers | Count of unique customers | `Total Customers = DISTINCTCOUNT(Sales[customer_id])` |
| Average Order Value | Sales per order | `Average Order Value = DIVIDE([Total Sales], [Total Orders])` |
| Average Quantity per Order | Quantity per order | `Average Quantity per Order = DIVIDE([Total Quantity], [Total Orders])` |
| Average Unit Price | Average price charged | `Average Unit Price = AVERAGE(Sales[unit_price])` |
| Total Discount Amount | Estimated discount dollars | `Total Discount Amount = SUMX(Sales, Sales[quantity] * Sales[unit_price] * Sales[discount_pct] / 100)` |
| Average Discount % | Average discount rate | `Average Discount % = AVERAGE(Sales[discount_pct])` |

## Order status and fulfillment measures

| Measure | Description | DAX |
|---|---|---|
| Shipped Orders | Orders with shipped status | `Shipped Orders = CALCULATE([Total Orders], Sales[order_status] = "shipped")` |
| Cancelled Orders | Orders cancelled | `Cancelled Orders = CALCULATE([Total Orders], Sales[order_status] = "cancelled")` |
| Fulfillment Rate | Share of shipped orders | `Fulfillment Rate = DIVIDE([Shipped Orders], [Total Orders])` |
| Cancel Rate | Share of cancelled orders | `Cancel Rate = DIVIDE([Cancelled Orders], [Total Orders])` |

## Time intelligence measures

| Measure | Description | DAX |
|---|---|---|
| Sales YTD | Year-to-date sales | `Sales YTD = TOTALYTD([Total Sales], Date[Date])` |
| Sales MTD | Month-to-date sales | `Sales MTD = TOTALMTD([Total Sales], Date[Date])` |
| Sales Last 12 Months | Rolling 12-month sales | `Sales Last 12 Months = CALCULATE([Total Sales], DATESINPERIOD(Date[Date], MAX(Date[Date]), -12, MONTH))` |
| Sales Same Period LY | Same period last year sales | `Sales Same Period LY = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(Date[Date]))` |

## Customer performance measures

| Measure | Description | DAX |
|---|---|---|
| Orders per Customer | Average orders per customer | `Orders per Customer = DIVIDE([Total Orders], [Total Customers])` |
| Repeat Customers | Customers with more than one order | `Repeat Customers = CALCULATE(DISTINCTCOUNT(Sales[customer_id]), FILTER(VALUES(Sales[customer_id]), CALCULATE(DISTINCTCOUNT(Sales[order_id])) > 1))` |

## Promotion and SCD2 measures

| Measure | Description | DAX |
|---|---|---|
| Active Promotions | Current promotions count | `Active Promotions = CALCULATE(DISTINCTCOUNT(PromotionHistory[promotion_id]), PromotionHistory[current_flag] = TRUE())` |
| Promotion Discount Average | Average promotion discount | `Promotion Discount Average = AVERAGE(PromotionHistory[discount_pct])` |
| Promotion History Rows | Total promotion history records | `Promotion History Rows = COUNTROWS(PromotionHistory)` |
| SCD2 Current Rows | Current active sales history rows | `SCD2 Current Rows = CALCULATE(COUNTROWS(SalesHistory), SalesHistory[current_flag] = TRUE())` |
| SCD2 Total History Rows | Full history record count | `SCD2 Total History Rows = COUNTROWS(SalesHistory)` |

## Data quality measures

| Measure | Description | DAX |
|---|---|---|
| Latest ETL Run | Most recent ETL timestamp | `Latest ETL Run = MAX(ETLControl[count_timestamp])` |
| Quality Check Rows | Count of ETL control rows | `Quality Check Rows = COUNTROWS(ETLControl)` |
| Latest ETL Sales Table Count | Latest row count in ETL control | `Latest ETL Sales Table Count = CALCULATE(LASTNONBLANKVALUE(ETLControl[count_timestamp], ETLControl[Table_count]), LASTDATE(ETLControl[count_timestamp]))` |

## Recommended visuals

- KPI cards: `Total Sales`, `Total Orders`, `Total Customers`, `Average Order Value`
- Trend line: `Sales YTD`, `Sales Last 12 Months`
- Bar chart: `Sales by product_category`, `Sales by region`, `Sales by sales_channel`
- Matrix/table: top products by `Total Sales`
- Cards: `Active Promotions`, `SCD2 Current Rows`, `Latest ETL Run`

## Notes

- Tables should be imported from the Fabric lakehouse or Delta paths in the workspace.
- If a date table exists, connect it to `Sales[order_date]` for the time intelligence measures.
- Replace the table names used in these measures with the actual names in your Power BI data model.
