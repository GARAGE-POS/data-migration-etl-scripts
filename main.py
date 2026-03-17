from Template_Tables.sync_units import main as units
from Template_Tables.sync_amenities import main as amenities
from Template_Tables.sync_app_sources import main as app_sources
from Template_Tables.sync_cities import main as cities
from Template_Tables.sync_landmarks import main as landmarks
from Template_Tables.sync_payment_modes import main as payment_modes
from Template_Tables.sync_services import main as services
from Template_Tables.sync_makes import main as makes
from Template_Tables.sync_models import main as models
from Main_Modules.Accounts.accounts import main as accounts
from Main_Modules.Locations.locations import main as locations
from Main_Modules.Locations.location_settings import main as location_settings
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
from Main_Modules.Discounts.discounts import main as discounts
from Main_Modules.Discounts.discount_location import main as discount_locations
from Orders_Payments.Company.company_clients import main as company_clients
from Orders_Payments.Company.company_invoices import main as company_invoices
from Orders_Payments.Orders.orders import main as orders
from Orders_Payments.Orders.order_payments import main as order_payments
from Orders_Payments.Orders.order_line_items import main as order_line_items
from Orders_Payments.Orders.order_packages import main as order_packages
from Orders_Payments.Payments.account_payment import main as account_payment
from Invertory.Warehouses.warehouses import main as warehouses
from Invertory.Suppliers.suppliers import main as suppliers
from Invertory.Purchases.purchase_bills import main as purchase_bills
from Invertory.Purchases.purchase_bill_details import main as purchase_bill_details 
from Invertory.Purchases.purchase_orders import main as purchase_orders
from Invertory.Stocks.stocks import main as stocks
from Invertory.Stocks.stock_transfers import main as stock_transfers
from Invertory.Stocks.stock_transfer_details import main as stock_transfer_details
from Invertory.Reconciliations.reconciliations import main as reconciliations
from Invertory.Reconciliations.reconciliation_details import main as reconciliation_details
from Settings.Subscriptions.subscriptions import main as subscriptions
from Settings.Subscriptions.subscription_addons import main as subscription_addons
from Settings.Roles.account_role_claims import main as account_role_claims
from Settings.Roles.subuser_roles import main as subuser_roles



def main():
    

    user_id = 2089
    load_db = 1

    # # Template tables sync (ONE TIME RUN!)
    # makes()
    # models()
    # app_sources(load_db==1)
    # cities(load_db==1)
    # amenities(load_db==1)
    # landmarks(load_db==1)
    # payment_modes(load_db==1)
    # units(load_db==1)
    # services(load_db==1)

    # # Account & Locations
    accounts(user_id, load_db==1)
    account_payment(user_id, load_db==1)
    locations(user_id, load_db==1)
    location_settings(user_id, load_db==1)

    # # Users & Customers
    # users(user_id, load_db==1)
    # customers(user_id, load_db==1)
    # customer_locations(user_id, load_db==1)
    # bays(user_id, load_db==1)

    # # Roles & Permissions 
    # account_role_claims(user_id, load_db==1)
    # subuser_roles(user_id, load_db==1)
    
    # # Discounts
    # discounts(user_id, load_db==1)
    # discount_locations(user_id, load_db==1)

    # # Cars
    # cars(user_id, load_db==1)
    # car_locations(user_id, load_db==1)

    # # Product Management
    # categories(user_id, load_db==1)
    # items(user_id, load_db==1)
    # location_items(user_id, load_db==1)

    # # Packages
    # packages(user_id, load_db==1)
    # package_details(user_id, load_db==1)
    # location_packages(user_id, load_db==1)


    # # Orders
    # orders(user_id, load_db==1)
    # order_payments(user_id, load_db==1)
    # order_line_items(user_id, load_db==1)

    
    # # Company Clients
    # company_clients(user_id, load_db==1)
    # company_invoices(user_id, load_db==1)

    # # Warehouses & Suppliers
    # warehouses(user_id, load_db==1)
    # suppliers(user_id, load_db==1)

    # # Inventory
    # reconciliations(user_id, load_db==1)
    # purchase_orders(user_id, load_db==1)
    # purchase_bills(user_id, load_db==1) 
    # purchase_bill_details(user_id, load_db==1)
    # stocks(user_id, load_db==1)
    # stock_transfers(user_id, load_db==1)
    # stock_transfer_details(user_id, load_db==1)

    # # Subscriptions & AddOns 
    # subscriptions(user_id, load_db==1)
    # subscription_addons(user_id, load_db==1)







if __name__ == '__main__':
    main()