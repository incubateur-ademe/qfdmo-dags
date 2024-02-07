import io
import csv
from urllib.parse import urlparse

import requests
import pandas as pd
import re


def send_batch_to_api(batch):
    """
    Send a batch of CSV lines to the geocoding API and return the response.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["identifiant_unique", "adresse", "adresse_complement", "code_postal", "ville"])
    for element in batch:
        row = element.values()
        writer.writerow(row)
    output.seek(0)
    response = requests.post(
        "https://api-adresse.data.gouv.fr/search/csv/",
        files={"data": output.getvalue()},
        data={"columns": ["adresse", "ville", "adresse_complement"], "postcode": "code_postal"},
    )
    reader = csv.DictReader(io.StringIO(response.text))
    return [row for row in reader]


def process_search_api_response(element):
    """
    Process each element returned from the search API and update address information.

    Args:
        element (dict): A dictionary containing data from the search API response.

    Returns:
        tuple: The updated element and a boolean indicating if the status is not 'ok'.
    """
    is_non_ok = element["result_status"] != "ok"
    if not is_non_ok:
        # Update address and city from the response
        element["adresse"] = element["result_name"]
        element["ville"] = element["result_city"]
        element["code_postal"] = element["result_postcode"]
        element["adresse_complement"] = element["adresse_complement"]
        element["st_x"] = element["longitude"]
        element["st_y"] = element["latitude"]
    return element, is_non_ok


def load_table(table_name, engine):
    return pd.read_sql_table(table_name, engine)


def apply_normalization(df, normalization_map):
    """
    Apply normalization functions to the specified columns of a DataFrame.
    """
    # print(df['adresse'])

    #df = normalize_address(df)
    #   print(df['adresse'])
    for column, normalize_func in normalization_map.items():
        if column in df.columns:
            df[column] = df[column].apply(normalize_func)
        else:
            print(f"Column {column} not found in DataFrame.")
    return df


def normalize_address(df, batch_size=10000):
    """
    Normalize the addresses in the DataFrame using the Ban API.
    """
    # Determine the number of batches
    num_batches = len(df) // batch_size + (len(df) % batch_size > 0)

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size

        # Extract relevant columns for address normalization
        address_data = df.loc[start_idx:end_idx,
                       ["identifiant_unique", "adresse", "adresse_complement", "code_postal", "ville"]]
        address_list = address_data.to_dict(orient='records')

        # Send data to Ban API and receive normalized data
        normalized_data = send_batch_to_api(address_list)

        # Process each element from the normalized data
        for j, element in enumerate(normalized_data):
            updated_element, is_non_ok = process_search_api_response(element)
            if is_non_ok:
                pass

            df.loc[start_idx + j, "adresse"] = updated_element["adresse"]
            df.loc[start_idx + j, "ville"] = updated_element["ville"]
            df.loc[start_idx + j, "code_postal"] = updated_element["code_postal"]

    return df


def formatted_string(string: str) -> str:
    result = string.title().strip().replace("  ", " ").replace("/", "")

    if result.upper().startswith("SA "):
        result = "SA " + result[3:]
    if result.upper().startswith("SA."):
        result = "SA." + result[3:]
    if result.upper().endswith(" SA"):
        result = result[:-3] + " SA"

    if result.upper().startswith("SAS "):
        result = "SAS " + result[4:]
    if result.upper().startswith("SAS."):
        result = "SAS." + result[4:]
    if result.upper().endswith(" SAS"):
        result = result[:-4] + " SAS"

    for word in result.split(" "):
        if len(word) >= 3 and re.match("^[^aeiouAEIOU]+$", word):
            result = result.replace(word, word.upper())

    if result.upper().startswith("SARL "):
        result = "SARL " + result[5:]
    if result.upper().endswith(" SARL"):
        result = result[:-5] + " SARL"
    result = result.replace(" Sarl ", " SARL ")

    if result.upper().startswith("ETS "):
        result = "Éts " + result[4:]
    if result.upper().endswith(" ETS"):
        result = result[:-4] + " Éts"
    result = result.replace(" Ets ", " Éts ")

    result = result.replace("Boîte À Lire", "Boîte à lire")
    result = result.replace("D Or", "D'Or")

    return result


def save_to_database(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def get_difference_counts(df_differences):
    return df_differences.count()


def normalize_phone_number(phone_number):
    if pd.isnull(phone_number):
        return None
    # Remove all non-numeric characters
    cleaned_phone = re.sub(r"\D", "", phone_number)
    # You can add more formatting logic here if needed
    return cleaned_phone


def normalize_nom(value):
    if pd.isnull(value):
        return None
    return formatted_string(value)


def normalize_email(email):
    if pd.isnull(email):
        return None
    # Convert the email to lowercase
    return email.lower()


def normalize_url(url):
    if pd.isnull(url):
        return None
    try:
        parsed_url = urlparse(url)
        # Extract the network location part (domain)
        domain = parsed_url.netloc
        # Remove 'www.' if it's part of the domain
        domain = domain.replace("www.", "")
        return domain
    except Exception as e:
        print(f"Error parsing URL {url}: {e}")
        return None