import pandas as pd
import asyncio


class ExcelExporter:
    """
    Handles the exporting of database data to an Excel file asynchronously.

    This class provides a static method to retrieve data from a database, transform it
    using predefined column mappings, and write it to an Excel file. It is designed to
    work with asynchronous database connection pools and leverages pandas for data
    transformation and I/O operations.

    """

    @staticmethod
    async def export_to_excel(db_pool, output_file):
        """
        Asynchronously exports data from a database pool to an Excel file. The data is
        queried from a database table, transformed using predefined column mappings,
        and written to an Excel file. Any missing values in the data are replaced with
        a default placeholder. An exception is caught and logged if the export operation
        fails.

        :param db_pool:
            Async database connection pool. It is used to acquire connections for executing
            the database queries asynchronously.

        :param output_file:
            Path to the output Excel file as a string. The exported data will be saved into
            this file as a worksheet named "Listings".

        :return:
            None. The function performs an I/O operation to write an Excel file but does not
            return any value.

        :raises Exception:
            If there is an error during the database query or while transforming/writing the
            data to an Excel file, the error is caught, and a message is printed.
        """
        try:
            # Daten aus der Datenbank abfragen
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM library")

            # Daten in ein Pandas DataFrame umwandeln
            df = pd.DataFrame([dict(row) for row in rows])

            columns_mapping = {
                "Action(SiteID=Germany|Country=DE|Currency=EUR|Version=1193)": "Action",
                "Custom label (SKU)": "id",
                "Category name": "",
                "Title": "Title",
                "Relationship": "",
                "Relationship details": "",
                "Schedule Time": "",
                "P:ISBN": "ISBN",
                "P:EPID": "",
                "Start price": "Start_price",
                "Quantity": "Quantity",
                "Item photo URL": "photo",
                "VideoID": "",
                "Condition ID": "Condition_ID",
                "Description": "Description",
                "Format": "Format",
                "Duration": "Duration",
                "Buy It Now price": "",
                "Best Offer Enabled": "Best_Offer_Enabled",
                "Best Offer Auto Accept Price": "Best_Offer_Auto_Accept_Price",
                "Minimum Best Offer Price": "Minimum_Best_Offer_Price",
                "VAT%": "",
                "Immediate pay required": "",
                "Location": "Location",
                "Shipping service 1 option": "",
                "Shipping service 1 cost": "",
                "Shipping service 1 priority": "",
                "Shipping service 2 option": "",
                "Shipping service 2 cost": "",
                "Shipping service 2 priority": "",
                "Max dispatch time": "",
                "Returns accepted option": "",
                "Returns within option": "",
                "Refund option": "",
                "Return shipping cost paid by": "",
                "Shipping profile name": "Shipping_profile_name",
                "Return profile name": "Return_profile_name",
                "Payment profile name": "Payment_profile_name",
                "ProductCompliancePolicyID": "",
                "Regional ProductCompliancePolicies": "",
                "C:Autor": "Autor",
                "C:Buchtitel": "Buchtitel",
                "C:Sprache": "Sprache",
                "C:Thematik": "",
                "C:Buchreihe": "",
                "C:Genre": "",
                "C:Verlag": "Verlag",
                "C:Erscheinungsjahr": "Erscheinungsjahr",
                "C:Originalsprache": "",
                "C:Format": "CFormat",
                "C:Herstellungsland und -region": "",
                "C:Produktart": "Produktart",
                "C:Literarische Gattung": "",
                "C:Zielgruppe": "",
                "C:Signiert von": "Signiert_von",
                "C:Ausgabe": "Ausgabe",
                "C:Literarische Bewegung": "",
                "Product Safety Pictograms": "",
                "Product Safety Statements": "",
                "Product Safety Component": "",
                "Regulatory Document Ids": "",
                "Manufacturer Name": "",
                "Manufacturer AddressLine1": "",
                "Manufacturer AddressLine2": "",
                "Manufacturer City": "",
                "Manufacturer Country": "",
                "Manufacturer PostalCode": "",
                "Manufacturer StateOrProvince": "",
                "Manufacturer Phone": "",
                "Manufacturer Email": "",
                "Manufacturer ContactURL": "",
                "Responsible Person 1": "",
                "Responsible Person 1 Type": "",
                "Responsible Person 1 AddressLine1": "",
                "Responsible Person 1 AddressLine2": "",
                "Responsible Person 1 City": "",
                "Responsible Person 1 Country": "",
                "Responsible Person 1 PostalCode": "",
                "Responsible Person 1 StateOrProvince": "",
                "Responsible Person 1 Phone": "",
                "Responsible Person 1 Email": "",
                "Responsible Person 1 ContactURL": "",
            }

            

            # Nur die relevanten Spalten übernehmen und umbenennen
            df = df[list(columns_mapping.keys())]
            df.rename(columns=columns_mapping, inplace=True)

            # Fehlende Werte durch "Keine Angabe" ersetzen
            df.fillna("Keine Angabe", inplace=True)

            # Excel-Datei schreiben
            df.to_excel(output_file, index=False, sheet_name="Listings")
            print(f"Daten erfolgreich in {output_file} exportiert.")

        except Exception as e:
            print(f"Fehler beim Exportieren der Daten: {e}")

# Beispielnutzung (außerhalb der async-Funktion ausführen):
# await ExcelExporter.export_to_excel(db_pool, "output.xlsx")
