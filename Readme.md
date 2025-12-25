# Migration Notes

## Template Tables

### AppSources :
- `UserID` column should be renamed as `AccountID` in **V2**.

### Cities & Countries :
- Obsolete `CountryCode` in both tables.
- Missing fields such `TaxPercentage`, `District`, `Currency`, ...

### Makes :
- **9** records have `ImagePath` value of **'-1'**.

### Models :
- **631** records have `ImagePath` value of **'-1'**.



## Main Modules
### SubUsers :
- `NormalizedUserName` fields have been left empty cause it breaks the **Uniqueness** rule.

### Cars :
- The missing values in `CreatedDate` column have been filled as discussed with **Shariq**.
- **4385** records with missing `CreatedDate` and `LastUpdatedDate` have been filled with **2000-01-01**.
- The formating of the dates has been fixed since they were stored as *Strings* in **V1**.  

### CustomerLocations :
- **7** records with missing `LocationID` have been filled with the value **16**.


### Categories :
- **4** records with missing `LocationID` have been dropped during migration.

### Items :
- **51** records with missing `Cost`, `Prince`, `IsOpenItem` and `IsInventoryitem` have been filled with **0**.
- **1221** records with `ItemType` value of **0**, have been set to **other**.  

## Orders & Payments

### Orders :
- **2133** records **(~0.14%)** have missing `OrderTakerID`.

### OrderTechnicians :
- Missing records in `WorkerID` and `AssistantID` fields.


## Inventory

### Warehouses :
- **41%** of the records have missing `StoreLocationID` in **V1**. temporarily filled with **4**.

### PurchaseBill :
- **18800** records **(~81%)** with missing `PurchaseOrderID`. Temporarily filled with **4**.
- **3067** records with missing `SupplierID`. Temporarily filled with **4**.
- `AuditedByUserID` and `Attachments` Columns don't exist in **V1**. Temporarily filled with **0** and `ImagePath`.


## Settings

### Subscriptions :
- **1** record with missing `PackageInfoID` has been assigned **1**.
- **3** records with missing `ExpiryDate` have been assigned `CreatedDate` **+ 1 Year**.
