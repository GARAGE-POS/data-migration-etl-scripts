import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text




def get_custom(engine: Engine, columns:str | list[str], table: str, col_not_null: str | None = None) -> pd.DataFrame:
    if isinstance(columns, list):
        columns = str(columns).replace('[', '').replace(']', '').replace("'", '')
    if col_not_null:
        return pd.read_sql(f"SELECT {columns} FROM {table} WHERE {col_not_null} IS NOT NULL", engine)
    return pd.read_sql(f"SELECT {columns} FROM {table}", engine)


def get_accounts(engine: Engine, old_user_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_user_ids is not None:
        acc_ids = (0,0) + tuple(old_user_ids.values.tolist())
        return pd.read_sql(f"SELECT AccountID, OldUserID FROM app.Accounts WHERE OldUserID IN {acc_ids} AND OldUserID IS NOT NULL", engine)
    return pd.read_sql("SELECT AccountID, OldUserID FROM app.Accounts WHERE OldUserID IS NOT NULL", engine)



def get_locations(engine: Engine, old_location_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_location_ids is not None:
        loc_ids = (0,0) + tuple(old_location_ids.values.tolist())
        return pd.read_sql(f"SELECT LocationID, OldLocationID FROM app.Locations WHERE OldLocationID IN {loc_ids} AND OldLocationID IS NOT NULL", engine)
    return pd.read_sql("SELECT LocationID, OldLocationID FROM app.Locations WHERE OldLocationID IS NOT NULL", engine)


def get_users(engine: Engine, old_subuser_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_subuser_ids is not None:
        user_ids = (0,0) + tuple(old_subuser_ids.values.tolist())
        return pd.read_sql(f"SELECT Id, OldID FROM app.AspNetUsers WHERE UserType='User' AND OldID IN {user_ids} AND OldID IS NOT NULL", engine)
    return pd.read_sql("SELECT Id, OldID FROM app.AspNetUsers WHERE UserType='User' AND OldID IS NOT NULL", engine)



def get_customers(engine: Engine, old_customer_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_customer_ids is not None:
        cust_ids = (0,0) + tuple(old_customer_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT Id AS CustomerID, OldID FROM app.AspNetUsers WHERE UserType='Customer' AND OldID IN {cust_ids} AND OldID IS NOT NULL", engine)
    return pd.read_sql("SELECT Id AS CustomerID, OldID FROM app.AspNetUsers WHERE UserType='Customer' AND OldID IS NOT NULL", engine)



def get_makes(engine: Engine, old_make_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_make_ids is not None:
        make_ids = (0,0) + tuple(old_make_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT MakeID, OldMakeID FROM app.Makes WHERE OldMakeID IN {make_ids} AND OldMakeID IS NOT NULL", engine)
    return pd.read_sql("SELECT MakeID, OldMakeID FROM app.Makes WHERE OldMakeID IS NOT NULL", engine)


def get_orders(engine: Engine, old_order_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_order_ids is not None:
        order_ids = (0,0) + tuple(old_order_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT OrderID, OldOrderID FROM app.Orders WHERE OldOrderID IN {order_ids} AND OldOrderID IS NOT NULL", engine)
    return pd.read_sql("SELECT OrderID, OldOrderID FROM app.Orders WHERE OldOrderID IS NOT NULL", engine)

def get_cars(engine: Engine, old_car_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_car_ids is not None:
        car_ids = (0,0) + tuple(old_car_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT CarID, OldCarID FROM app.Cars WHERE OldCarID IN {car_ids}", engine)
    return pd.read_sql("SELECT CarID, OldCarID FROM app.Cars WHERE OldCarID IS NOT NULL", engine)

def get_order_details(engine: Engine, old_order_detail_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_order_detail_ids is not None:
        order_detail_ids = (0,0) + tuple(old_order_detail_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT OrderDetailID, OldOrderDetailID FROM app.OrderDetails WHERE OldOrderDetailID IN {order_detail_ids}", engine)
    return pd.read_sql("SELECT OrderDetailID, OldOrderDetailID FROM app.OrderDetails WHERE OldOrderDetailID IS NOT NULL", engine)

def get_items(engine: Engine, old_item_ids : pd.Series) -> pd.DataFrame:
    item_ids = tuple(old_item_ids.dropna().values.tolist()) + (0,0)
    query = text(f"""
        SELECT ItemID, OldItemID
        FROM app.SyncItems s
        JOIN app.Items i
            ON s.CategoryID = i.CategoryID 
                 AND s.Name COLLATE Latin1_General_CS_AS = i.Name COLLATE Latin1_General_CS_AS
        WHERE OldItemID IN {item_ids}
    """)
    return pd.read_sql(query, engine)

def get_categories(engine: Engine, old_cat_ids : pd.Series) -> pd.DataFrame:
    cat_ids = tuple(old_cat_ids.dropna().values.tolist()) + (0,0)
    query = text(f"""
            SELECT s.OldCategoryID, c.CategoryID
            FROM app.synccategories s
            JOIN app.categories c
                ON s.accountid = c.AccountID
                    AND c.Name COLLATE Latin1_General_CS_AS = s.Name COLLATE Latin1_General_CS_AS
            WHERE OldCategoryID IN {cat_ids}
            ORDER BY s.OldCategoryID
    """)
    return pd.read_sql(query, engine)

def get_cities(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT CountryID, CityID, OldCityID FROM app.SyncCities", engine)

def get_suppliers(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT SupplierID, OldSupplierID FROM app.Suppliers WHERE OldSupplierID IS NOT NULL", engine)

def get_packages(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT PackageID, OldPackageID FROM app.Packages WHERE OldPackageID IS NOT NULL", engine)

def get_warehouses(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT WarehouseID, OldStoreID FROM app.Warehouses WHERE OldStoreID IS NOT NULL", engine)

def get_stock_transfers(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(f"SELECT TransferID AS StockTransferID, OldStockIssueID FROM app.StockTransfers", engine)




