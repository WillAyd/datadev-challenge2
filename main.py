import argparse
import getpass
import logging
import re
import mail

from datetime import time
import pandas as pd
import requests

import tableauserverclient as TSC


def main():
    # Set the query https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_examples.html
    query = """
    {
        workbooks{
            id
            name
            owner{
                name
                email
            }
            embeddedDatasources{
                name
                fields{
                    __typename
                    name
                }
            }
        }
    }
    """

    parser = argparse.ArgumentParser(
        description="Creates sample schedules for each type of frequency."
    )
    parser.add_argument("--server", "-s", required=True, help="server address")
    parser.add_argument(
        "--username", "-u", required=True, help="username to sign into server"
    )
    parser.add_argument(
        "--logging-level",
        "-l",
        choices=["debug", "info", "error"],
        default="error",
        help="desired logging level (set to error by default)",
    )
    parser.add_argument("--sitename", "-n", help="fghdhr")
    args = parser.parse_args()

    password = getpass.getpass("Password: ")    

    # Set logging level based on user input, or error by default
    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level)

    tableau_auth = TSC.TableauAuth(args.username, password, args.sitename)
    server = TSC.Server(args.server)
    server.version = "3.3"

    with server.auth.sign_in(tableau_auth):
        # Query the Metadata API and store the response in resp
        resp = server.metadata.query(query)
        datasources = resp["data"]
    
    # TODO: we should be able to provide a meta path for ["workbooks", "owner", "name"]
    # and ["workbooks", "owner", "email"] but seems to be a bug in pandas atm
    df = pd.json_normalize(
        datasources,
        record_path=["workbooks", "embeddedDatasources", "fields"],
        meta=[
            ["workbooks", "embeddedDatasources", "name"],
            ["workbooks", "name"],
            ["workbooks", "owner"],
        ],
        record_prefix='field_',
        sep='_',
        errors='ignore'
    )

    df["owner"] = df["workbooks_owner"].apply(lambda x: x["name"])
    df["email"] = df["workbooks_owner"].apply(lambda x: x["email"])
    df = df.rename(
        columns={
            "workbooks_embeddedDatasources_name": "datasource_name",
            "workbooks_name": "workbook_name",
            "field___typename": "field_type"
        }
    ).drop(columns=["workbooks_owner"])
    print(f"column types:\n{df.dtypes}")

    # TODO: should we add patterns like "TEST" or "DELETE"?
    pater = r"^Calculation \d+$"
    newdf = df[df["field_name"].str.match(pater)]
    
    # build email body with the list of bad fields to inform the owner 
    emaildf = newdf[["workbook_name","datasource_name","field_name"]]

    html = """\
    <html>
    <head></head>
    <body>
        <P>Please rename the following calculated fields as per standard naming conventions</P>
        {0}
    </body>
    </html>
    """.format(emaildf.to_html(index=False))

    mail.send_email("user email address", html)


if __name__ == "__main__":
    main()
