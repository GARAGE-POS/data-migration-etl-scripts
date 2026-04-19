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
        return pd.read_sql(f"SELECT CarID, OldCarID FROM app.Cars WHERE OldCarID IN {car_ids} AND OldCarID IS NOT NULL", engine)
    return pd.read_sql("SELECT CarID, OldCarID FROM app.Cars WHERE OldCarID IS NOT NULL", engine)

def get_order_details(engine: Engine, old_order_detail_ids: pd.Series | None = None) -> pd.DataFrame:
    if old_order_detail_ids is not None:
        order_detail_ids = (0,0) + tuple(old_order_detail_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT LineItemID AS OrderDetailID, OldOrderDetailID FROM app.OrderLineItems WHERE OldOrderDetailID IN {order_detail_ids} AND OldOrderDetailID IS NOT NULL", engine)
    return pd.read_sql("SELECT LineItemID AS OrderDetailID, OldOrderDetailID FROM app.OrderLineItems WHERE OldOrderDetailID IS NOT NULL", engine)

def get_items(engine: Engine, old_item_ids : pd.Series | None = None) -> pd.DataFrame:
    if old_item_ids is not None:
        item_ids = (0,0) + tuple(old_item_ids.dropna().values.tolist())
        return pd.read_sql(f"SELECT ItemID, OldItemID FROM app.Items WHERE OldItemID IN {item_ids} AND OldItemID IS NOT NULL", engine)
    return pd.read_sql("SELECT ItemID, OldItemID FROM app.Items WHERE OldItemID IS NOT NULL", engine)

def get_categories(engine: Engine, old_cat_ids : pd.Series) -> pd.DataFrame:
    cat_ids = tuple(set(old_cat_ids.dropna().values.tolist())) + (0,0)
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


def get_permissions(engine: Engine, roles: str | list[str] | None = None) -> pd.DataFrame:
    if roles is None:
        return pd.read_sql("SELECT * FROM app.RolesAndPermissions", engine)
    if isinstance(roles, str):
        return pd.read_sql(f"SELECT * FROM app.RolesAndPermissions WHERE Role={roles}", engine)
    return pd.read_sql(f"SELECT * FROM app.RolesAndPermissions WHERE Role IN {'(' + str(roles)[1:-1] + ')'}", engine)

def get_cities(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT CountryID, CityID, OldCityID FROM app.SyncCities", engine)

def get_suppliers(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT SupplierID, OldSupplierID FROM app.Suppliers WHERE OldSupplierID IS NOT NULL", engine)

def get_packages(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT PackageID, OldPackageID FROM app.Packages WHERE OldPackageID IS NOT NULL", engine)

def get_warehouses(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT WarehouseID, OldStoreID FROM app.Warehouses WHERE OldStoreID IS NOT NULL", engine)

def get_stock_transfers(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(f"SELECT TransferID AS StockTransferID, OldStockIssueID FROM app.StockTransfers WHERE OldStockIssueID IS NOT NULL", engine)

def get_addons(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT AddOnID, AddOnName FROM app.AddOns", engine)

def get_bays(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT BayID, OldBayID FROM app.Bays WHERE OldBayID IS NOT NULL", engine)


def get_discounts(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT DiscountID, OldDiscountID FROM app.Discounts WHERE OldDiscountID IS NOT NULL", engine)


def get_company_clients(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT CompanyClientID, OldCompanyClientID FROM app.CompanyClients WHERE OldCompanyClientID IS NOT NULL", engine)


def get_company_quotations(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT CompanyQuotationID, OldQuotationID FROM app.CompanyQuotations WHERE OldQuotationID IS NOT NULL", engine)