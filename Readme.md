# How to Run the Data Migration Script

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the root directory with your database connections:

```env
# Source DB
AZURE_SERVER=localhost
AZURE_DATABASE=source_db
AZURE_USERNAME=user
AZURE_PASSWORD=password

# Target DB
STAGE_SERVER=localhost
STAGE_DATABASE=target_db
STAGE_USERNAME=user
STAGE_PASSWORD=password


NU_PASSWORD=subusers_new_default_password (hashed)
SEC_STAMP=security_stamp
```


### 3. Configure Script
Edit `main.py`:

```python
user_id = 2089   # User to migrate
load_db = 0      # 0 = dry run, 1 = write to DB
```

Control which migrations run by commenting/uncommenting:

```python
accounts(user_id, load_db==1)
account_payment(user_id, load_db==1)
# locations(user_id, load_db==1)
# location_settings(user_id, load_db==1)
```

### 4. Incremental Sync (`last_ingested.json`)
The script maintains a `last_ingested.json` file to track the last processed record ID per dataset.

- On each run, only **new records** (IDs greater than the last ingested) are migrated
- This allows safe re-runs while the source system is still receiving data
- The file is automatically updated after successful processing

**Reset behavior:**
- Modify `last_ingested.json` to reprocess data from an earlier point or from scratch

**Note that the tracking doesn't work for `Accounts`, `AccountPayments`, `Subscriptions`, `SubscriptionAddons` migrations**


### 5. Run the Script
```bash
python main.py
```

# Migration Notes

## Template Tables

### Makes :
- **9** records have `ImagePath` value of **"-1"**.
- **24** records in **Cars** is affected by incorrectly encoded `Name` *(ie: "???")*
- **0** records in **Cars** is affected by **"."**
- **5** records in **Cars** is affected by **"Bajaj"**
- **1** records in **Cars** is affected by **"Chinese"**
- **5371** records in **Cars** is affected by **"EBRAQ"**
- **1** records in **Cars** is affected by **"Haojue"**
- **2** records in **Cars** is affected by **"HELI"**
- **0** records in **Cars** is affected by **"Hero"**
- **3** records in **Cars** is affected by **"INDIAN"**
- **0** records in **Cars** is affected by **"New Svg"**
- **0** records in **Cars** is affected by **"rafu"**
- **7** records in **Cars** is affected by **"Robi"**
- **0** records in **Cars** is affected by **"Saniya"**
- **0** records in **Cars** is affected by **"Sun"**
- **31** records in **Cars** is affected by **"Tank"**
- **0** records in **Cars** is affected by **"test%"** 
- **25** records in **Cars** is affected by **" TVS"**
- **10** records in **Cars** is affected by **"UD"**
- **0** records in **Cars** is affected by **"VEHICLEC LOADER"**
- **11** records in **Cars** is affected by **"XXXX"**
- **9** records in **Cars** is affected by **"CHAIRMAN"**


### Models :
- **631** records have `ImagePath` value of **"-1"**.



## Main Modules
### SubUsers :
- `NormalizedUserName` fields have been left empty for duplicates cause it breaks the **Uniqueness** rule.

### Locations :
- **10** records with missing `CityID` and `CountryID` of **"SA""** have been filled with **4101** temporarily.
- **1899** records with missing `LandmarkID`, and **12** records with values different than **(1,2)** have been set to **Null**.

### Cars :
- The missing values in `CreatedDate` column have been filled as discussed with **Shariq**.
- **4385** records with missing `CreatedDate` and `LastUpdatedDate` have been filled with **2000-01-01**.
- The formating of the dates has been fixed since they were stored as *VARCHAR* in **V1**. 
- `Odometer`, `FuelType` and `CarPlateType` columns are missing.

### CustomerLocations :
- **7** records with missing `LocationID`.


### Categories :
- **4** records with missing `LocationID` have been dropped during migration.

### Items :
- **51** records with missing `Cost`, `Price`, `IsOpenItem` and `IsInventoryitem` have been filled with **0**.
- `ItemTypeID` is set to **1**.  

### Packages :
- **3NF** is violeted since `AccountID` depends on `CategoryID`.

## Orders & Payments

### Orders :
- **425** record with negative `AmountTotal`, `AmountPaid` or `GrandTotal`.
- **2133** records **(~0.14%)** have missing `OrderTakerID`. The constraint changed to allow nulls.
- `AmountTotal` mapped to `Subtotal` and `GrandTotal` mapped to `Total`.
- All missing records from **OrderCheckout table**'s fields have been filled with **0**.
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
- **41%** of the records have missing `StoreLocationID` in **V1**. Filled with the first location of the given user.

### PurchaseBill :
- **18800** records **(~81%)** with missing `PurchaseOrderID`. 
- **3067** records with missing `SupplierID`. Filled with a supplier of the given user.
- `Attachments` Columns don't exist in **V1**. Filled with `ImagePath`.
- Constraint of `AuditedByUserID` changed to allow nulls.
- `ReferenceNumber` and `BillNumber` changed to ensure uniqueness constraint.


