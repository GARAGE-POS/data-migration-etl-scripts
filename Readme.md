# Migration Notes

## Template Tables

### AppSources :
- `UserID` field has been filled with the value **2** temporarily.

### Cities & Countries :
- Obsolete `CountryCode` in both tables.
- Missing fields such `TaxPercentage`, `District`, `Currency`, ...

### Makes :
- **9** records have `ImagePath` value of **"-1"**.
- **24** records in **Cars Tables** is affected by incorrectly encoded `Name` *("???")*
- **0** records in **Cars Tables** is affected by **"."**
- **5** records in **Cars Tables** is affected by **"Bajaj"**
- **1** records in **Cars Tables** is affected by **"Chinese"**
- **5371** records in **Cars Tables** is affected by **"EBRAQ"**
- **1** records in **Cars Tables** is affected by **"Haojue"**
- **2** records in **Cars Tables** is affected by **"HELI"**
- **0** records in **Cars Tables** is affected by **"Hero"**
- **3** records in **Cars Tables** is affected by **"INDIAN"**
- **0** records in **Cars Tables** is affected by **"New Svg"**
- **0** records in **Cars Tables** is affected by **"rafu"**
- **7** records in **Cars Tables** is affected by **"Robi"**
- **0** records in **Cars Tables** is affected by **"Saniya"**
- **0** records in **Cars Tables** is affected by **"Sun"**
- **31** records in **Cars Tables** is affected by **"Tank"**
- **0** records in **Cars Tables** is affected by **"test%"** 
- **25** records in **Cars Tables** is affected by **" TVS"**
- **10** records in **Cars Tables** is affected by **"UD"**
- **0** records in **Cars Tables** is affected by **"VEHICLEC LOADER"**
- **11** records in **Cars Tables** is affected by **"XXXX"**

### Models :
- **631** records have `ImagePath` value of **"-1"**.



## Main Modules
### SubUsers :
- `NormalizedUserName` fields have been left empty cause it breaks the **Uniqueness** rule.

### Locations :
- **10** records with missing `CityID` and `CountryID` of **"SA""** have been filled with **4101** temporarily.
- **1899** records with missing `LandmarkID`, and **12** records with values different than **(1,2)** have been set to **Null**.

### Cars :
- The missing values in `CreatedDate` column have been filled as discussed with **Shariq**.
- **4385** records with missing `CreatedDate` and `LastUpdatedDate` have been filled with **2000-01-01**.
- The formating of the dates has been fixed since they were stored as *VARCHAR* in **V1**. 
- `Odometer`, `FuelType` and `CarPlateType` columns are missing.

### CustomerLocations :
- **7** records with missing `LocationID` have been filled with the value **16**.


### Categories :
- **4** records with missing `LocationID` have been dropped during migration.

### Items :
- **51** records with missing `Cost`, `Price`, `IsOpenItem` and `IsInventoryitem` have been filled with **0**.
- **1221** records with `ItemType` value of **0**, have been set to **"other"**.  

### Packages :
- **3NF** is violeted since `AccountID` depends on `CategoryID`.

## Orders & Payments

### Orders :
- **2133** records **(~0.14%)** have missing `OrderTakerID`.
- `AmountTotal` mapped to `Subtotal` and `GrandTotal` mapped to `Total`, yet these columns needs more handling such that `Tax` =  (`Subtotal` - `DiscountAmount`) x **Rate**.
- All missing records from **OrderCheckout table**'s fields have been filled with **0**.
- `ServiceStatusID` is filled temporarily till getting the correct values from **Abo Mussa**.
- **16112** records have `ServiceCharges` value of **-1**. 
- **3NF** is violeted since `CustomerID` depends on `CarID`.


### Payments :


### OrderTechnicians :
- Missing records in `WorkerID` and `AssistantID` fields.


## Inventory

### Stocks :
- **31093** records have missing `CurrentStock`.
- **233** records have missing `ItemID`.
- Unclear Mapping 

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
