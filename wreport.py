import streamlit as st
import pandas as pd
import gspread
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from datetime import datetime

# ========== Google Service Account credentials ==========
SERVICE_ACCOUNT_CREDS = {
    "type": "service_account",
    "project_id": "offline-387317",
    "private_key_id": "5e986378f1c64270d87db569738d8db7cb9d5d93",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQClYtYfBHetdbBr\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "yehia-831@offline-387317.iam.gserviceaccount.com",
    "client_id": "110126758688847320414",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/yehia-831%40offline-387317.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_CREDS, scopes=SCOPES)
gc = gspread.authorize(credentials)

# ========== Streamlit UI ==========
st.title("wreport - Weekly Sheet Updater")
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")
weekly_over = st.text_input("Weekly Over Limit")

if st.button("Update Sheets"):
    try:
        sh = gc.open_by_key('1QOtyGgZYENiC-o4pnTZi8UZfxunZyNLeOpOtJeFSyKY')
        start = start_date.strftime('%Y-%m-%d')
        end = end_date.strftime('%Y-%m-%d')

        sheet = sh.add_worksheet(title=f"Power vs. Collected {start_date}", rows=30, cols=10)

        ops = create_engine("postgresql://analysis_team:ZvVU9ajncL@ops-management-db.statsbomb.com:5432/ops_management")
        query = f"""
        SELECT case 
            when s."name" = 'A`' then 'A'
            else s."name"
        end as squad,
                    SUM(ssm.power) AS power
        FROM users u
        LEFT JOIN squads_shifts_members ssm ON u.id = ssm.user_id
        LEFT JOIN squads_shifts ss ON ssm.squad_shift_id = ss.id
        LEFT JOIN squads s ON u.squad_id = s.id
        WHERE ssm.vacation_type IN ('PRESENT', 'PRESENT_DS', 'PRESENT_ES', 'PRESENT_TA')
          and ss."date" between '{start}' and '{end}'
          AND ssm.power > 0
          AND LENGTH(s.name) <= 2
        GROUP BY 1
        ORDER BY 1;
        """
        df = pd.read_sql(query, ops)

        sheet.batch_clear(["A:B"])
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        sheet.update_acell('C1', 'Collected')
        sheet.update_acell('C2', '=ARRAYFORMULA(...)')  # Simplified for brevity
        sheet.update_acell('D1', 'DIF')
        sheet.update_acell('D2', '=ARRAYFORMULA(C2:C-B2:B)')
        sheet.update_acell('E1', 'Over/Deficit %')
        sheet.update_acell('E2', '=ARRAYFORMULA(IFERROR(D2:D/B2:B))')
        sheet.update_acell('F1', 'Bonus Multiplier')
        sheet.update_acell('F2', '=ARRAYFORMULA(...)')  # Full formula here
        sheet.update_acell('G1', 'Overtarget Limit Status')
        sheet.update_acell('G2', '=ARRAYFORMULA(IF(D2:D>$I$2,"Above the limit",""))')
        sheet.update_acell('H1', 'Over/Deficit Reason')
        sheet.update_acell('I1', start)
        sheet.update_acell('I2', weekly_over)
        sheet.columns_auto_resize(0, 9)

        sheet2 = sh.add_worksheet(title=f"Collected {start_date}", rows=1000, cols=7)

        ct = create_engine("postgresql://matchstatus_ro:98aaFHA7sgS66fd@primary-db-prod.cluster-cpnvwmhjbrie.eu-west-2.rds.amazonaws.com:5432/matchstatus")
        query2 = f"""
        select arqam_id,cast(completion_time as varchar),cast(first_import as varchar), cast(last_import as varchar) from (
        with query as (
        select arqam_id, sbd_id, iq_id, cast(matches."date" as varchar) match_date, kick_off_time,type , "dateTime"
        from matches,
        jsonb_to_recordset(matches.history) as x("type" text, "dateTime" timestamp, message json)
        where arqam_id notnull
        order by "dateTime"
        )
        select arqam_id,
        min (case when type = 'COLLECTION_COMPLETE' then date_trunc('second',"dateTime")+ interval '2 hour' end) as completion_time,
        min(case when type = 'IQ_IMPORT_SUCCESS' then date_trunc('second',"dateTime")+ interval '2 hour' else null end) as first_import,
        max(case when type = 'IQ_IMPORT_SUCCESS' then date_trunc('second',"dateTime")+ interval '2 hour' else null end) as last_import
        from query group by arqam_id
        ) as t1
        where t1.completion_time >= '2025-01-01'
        """
        df2 = pd.read_sql(query2, ct)

        sheet2.clear()
        sheet2.update([df2.columns.values.tolist()] + df2.values.tolist())
        sheet2.update_acell('E1', 'Squad')
        sheet2.update_acell('F1', 'Assignment Date')
        sheet2.update_acell('G1', 'Comment')
        sheet2.update_acell('E2','=ARRAYFORMULA(IFNA(VLOOKUP(A2:A, squads!A:B, 2, FALSE)))')
        sheet2.update_acell('F2','=ARRAYFORMULA(IFERROR(DATEVALUE(IFNA(VLOOKUP(A2:A, squads!A:C, 3, FALSE)))))')

        sheet3 = sh.worksheet('squads')
        query3 = f"""
        SELECT m.id,
               case when s.name = 'A`' then 'A'
               else s.name
               end as squad,                
               CAST(ss.date as varchar) assignment_date
        FROM matches m
        JOIN squad_shift_matches ssm ON ssm.match_id = m.id
        JOIN squads_shifts ss ON ss.id = ssm.squad_shift_id
        JOIN squads s ON s.id = ss.squad_id
        WHERE ss."date" >= '2024-01-01'
        """
        df3 = pd.read_sql(query3, ops)
        sheet3.batch_clear(["A:C"])
        sheet3.update([df3.columns.values.tolist()] + df3.values.tolist())

        st.success("Sheets updated successfully!")
    except Exception as e:
        st.error(f"An error occurred: {e}")
