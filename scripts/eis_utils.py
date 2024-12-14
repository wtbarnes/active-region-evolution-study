"""
Utilities for dealing with EIS data
"""
import sqlite3

import astropy.table
import astropy.time
from sunpy.data import manager
import sunpy.time  # Register TAI seconds format


@manager.require(
        'eis_database',
        ['https://hesperia.gsfc.nasa.gov/ssw/hinode/eis/database/catalog/eis_cat.sqlite'],
        '5336191e89ee2b648d28684c3fb282f5a7f7ed1c1c7806879d700efd4671662c',
)
def get_eis_catalogue():
    """
    Return the EIS observation catalogue as a dictionary of `~astropy.table.QTable` objects
    
    Parameters
    ----------
    filename
    """
    filename = manager.get('eis_database')
    con = sqlite3.connect(filename)
    # Get all tablenames
    table_names = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = {}
    for t in table_names:
        # Get column names
        cursor = con.execute(f"SELECT * FROM {t[0]}")
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        table = astropy.table.QTable(rows=rows, names=columns)
        # Format some columns. This will probably have to vary per table. I'm not sure
        # what other columns outside of the eis_experiment table need to be formatted
        if t[0] == 'eis_experiment' or t[0] == 'eis_main':
            for c in ['xcen', 'ycen', 'fovx', 'fovy']:
                table[c].unit = 'arcsec'
            table['xcen'] = table['xcen'].astype(float)
            table['ycen'] = table['ycen'].astype(float)
            table['date_obs'] = astropy.time.Time(table['date_obs'], format='tai_seconds', scale='tai')
            table['date_end'] = astropy.time.Time(table['date_end'], format='tai_seconds', scale='tai')
            table['date_avg'] = table['date_obs'] + (table['date_end'] - table['date_obs']) / 2
        tables[t[0]] = table
    con.close()
    return tables
