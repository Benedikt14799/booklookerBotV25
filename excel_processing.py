import pandas as pd
import asyncio


class ExcelExporter:
    """
    Klasse: ExcelExporter
    --------------------------
    Exportiert die Daten aus der `library`-Datenbanktabelle in eine Excel-Datei
    im gewünschten Format.
    MAPPING = {
    "Excel-Spaltenname": "Datenbank-Spaltenname"
    }
    """

    @staticmethod
    async def export_to_excel(db_pool, output_file):
        """
        Funktion: export_to_excel
        -------------------------
        Extrahiert Daten aus der `library`-Tabelle und schreibt sie in eine Excel-Datei.

        Parameter:
        - db_pool: Verbindung zur Datenbank.
        - output_file (str): Der Pfad zur Ausgabe-Excel-Datei.
        """
        try:
            # Daten aus der Datenbank abfragen
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM library")

            # Daten in ein Pandas DataFrame umwandeln
            df = pd.DataFrame([dict(row) for row in rows])

            # Spaltenmapping auf die gewünschten Spalten der Vorlage anwenden
            columns_mapping = { #ToDo: Mapping anpassen
                "Action": "",
                "Custom label / SKU": "",
                "Category Name": "",
                "Title": "",
                "Relationship": "",
                "Relationship Details": "",
                "ISBN": "",
                "EPID": "",
                "Start price": "",
                "Quantity": "",
                "Photo": "",
                "Video ID": "",
                "Condition ID": "",
                "Description": "",
                "Format": "",
                "Duration": "",
                "Buy It Now price": "",
                "Best Offer Enabled": "",
                "Best Offer Auto Accept Price": "",
                "Minimum Best Offer Price": "",
                "VAT percent": "",
                "Immediate pay required": "",
                "Location": "",
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
                "Shipping profile name": "",
                "Return profile name": "",
                "Payment profile name": "",
                "Product Compliance Policy ID": "",
                "Regional Product Compliance Policies": "",
                "Economic Operator - Company Name": "",
                "Economic Operator - Address Line 1": "",
                "Economic Operator - Address Line 2": "",
                "Economic Operator - City": "",
                "Economic Operator - Country": "",
                "Economic Operator - Postal Code": "",
                "Economic Operator - State or Province": "",
                "Economic Operator - Phone": "",
                "Economic Operator - Email": "",
                "Author": "",
                "Book Title": "",
                "Language": "",
                "Keywords": "",
                "Book Series": "",
                "Genre": "",
                "Publisher": "",
                "Year of Publication": "",
                "Format (Dimensions)": "",
                "Original Language": "",
                "Country/Region of Manufacture": "",
                "Product Type": "",
                "Literary Movement": "",
                "Signed By": "",
                "Literary Genre": "",
                "Target Audience": "",
                "Edition": ""
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
