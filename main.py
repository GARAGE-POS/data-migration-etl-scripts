from Template_Tables.units import main as units
from Main_Modules.Accounts.accounts import main as accounts
from Main_Modules.Locations.locations import main as locations
from Main_Modules.Bays.bays import main as bays
from Main_Modules.AspNetUsers.subusers import main as users
from Main_Modules.AspNetUsers.customers import main as customers
from Main_Modules.AspNetUsers.customer_locations import main as customer_locations
from Main_Modules.ProductManagement.categories import main as categories
from Main_Modules.ProductManagement.items import main as items
from Main_Modules.ProductManagement.location_items import main as location_items
from Main_Modules.Packages.packages import main as packages
from Main_Modules.Packages.package_details import main as package_details
from Main_Modules.Packages.location_packages import main as location_packages
from Main_Modules.Cars.cars import main as cars
from Main_Modules.Cars.car_locations import main as car_locations
from Orders_Payments.Payments.account_payment import main as account_payment
from Invertory.Warehouses.warehouses import main as warehouses
from Invertory.Suppliers.suppliers import main as suppliers
from Invertory.Purchases.purchase_bills import main as purchase_bills
from Invertory.Purchases.purchase_bill_details import main as purchase_bill_details 
from Invertory.Purchases.purchase_orders import main as purchase_orders
from Invertory.Stocks.stock_transfers import main as stock_transfers
from Invertory.Stocks.stock_transfer_details import main as stock_transfer_details
from Invertory.Reconciliations.reconciliations import main as reconciliations
from Settings.Subscriptions.subscriptions import main as subscriptions



def main():

    # accounts()
    # subscriptions()
    locations()

    # customers()
    # cars()




if __name__ == '__main__':
    main()