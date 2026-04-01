from Template_Tables.sync_units import main as units
from Template_Tables.sync_amenities import main as amenities
from Template_Tables.sync_app_sources import main as app_sources
from Template_Tables.sync_cities import main as cities
from Template_Tables.sync_landmarks import main as landmarks
from Template_Tables.sync_payment_modes import main as payment_modes
from Template_Tables.sync_services import main as services
from Template_Tables.sync_makes import main as makes
from Template_Tables.sync_models import main as models
from Setup.setup import main as setup


def main():

    load_db=1

    setup()
    makes()
    models()
    app_sources(load_db==1)
    cities(load_db==1)
    amenities(load_db==1)
    landmarks(load_db==1)
    payment_modes(load_db==1)
    units(load_db==1)
    services(load_db==1)


if __name__ == '__main__':
    main()