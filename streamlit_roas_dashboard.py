import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import psycopg2
import altair as alt
import streamlit as st
import io
import calendar
from st_aggrid import GridOptionsBuilder, AgGrid, JsCode
from millify import prettify
import time
st.set_page_config(layout="wide")

st.markdown("<h1 style='text-align: center; color: rgb(204, 143, 37);'>PlusMinusOne ROAS-LTV Dashboard</h1>", unsafe_allow_html=True)
st.write("")
st.write("")
st.write("")
st.write("")

# Streamlit secrets'den veritabanı bilgilerini çekmek
db_info = st.secrets["postgresql"]

# Veritabanı bağlantısı kurma
conn = psycopg2.connect(
    host=db_info["host"],
    database=db_info["database"],
    user=db_info["user"],
    password=db_info["password"]
)

# Cursor oluşturma
cursor = conn.cursor()

print("SUCCESSFUL DB CONNECTION")

@st.cache_data
def read_sql_query_io(query):

    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query = query, head = "HEADER")
    store = io.StringIO()
    cursor.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)

    return df

st.toast('Cooking...', icon='👨🏼‍🍳')

@st.cache_data
def packages_query():
    packages_query = """ 
    SELECT * 
    FROM "package_details" 
    """
    
    df_packages = read_sql_query_io(packages_query)
    print("PACKAGE QUERY OK")

    return df_packages

df_packages = packages_query()

@st.cache_data
def countries_query():
    countries_query = "SELECT * from countries"
    df_countries = read_sql_query_io(countries_query)
    print("COUNTRIES QUERY OK")

    global_row = pd.DataFrame({"countryname" : ['GLOBAL'], "countrycode" : ['GLOBAL']})
    df_countries = pd.concat([global_row, df_countries]).reset_index(drop = True)

    return df_countries

df_countries = countries_query()

packageDetailsQuery = """ 
SELECT *
FROM "package_details"
"""
package_details_df = read_sql_query_io(packageDetailsQuery)
print("DONE PACKAGE DETATILS DF")



def predicted_ltv(country_option):
    if country_option == 'GLOBAL':
        country_filter = ''
    else:
        country_code = df_countries[df_countries['countryname'] == country_option]['countrycode'].unique()[0]
        country_filter = 'AND "country" = \'{}\''.format(country_code)
    app_store_connect_events_query = """        
    SELECT "originalStartDate",
        "eventDate",
        "event",
        "appAppleId",
        "subscriptionAppleId",
        "country",
        "proceedsReason",
        "consecutivePaidPeriods",
        SUM("quantity") as "quantity"
    FROM subscription_event
    WHERE "event" IN ('Subscribe',
                    'Paid Subscription from Introductory Offer',
                    'Reactivate',
                    'Refund',
                    'Renew',
                    'Renewal from Billing Retry',
                    'Renewals from Grace Period',
                    'Reactivate with Crossgrade',
                    'Reactivate with Downgrade',
                    'Reactivate with Upgrade')
    AND "appAppleId" = '{}'
    {} -- Ülke filtresi burada ekleniyor
    GROUP BY "originalStartDate", "eventDate", "event", "appAppleId", "subscriptionAppleId", "proceedsReason", "consecutivePaidPeriods","country"
    """.format(app_id, country_filter)
    events_df = read_sql_query_io(app_store_connect_events_query)
    print("GLOBAL DONE APP STORE CONNECT EVENTS DF")

    final_df = events_df.merge(package_details_df, 
            how='left',
            on = ['appAppleId', 'subscriptionAppleId'])
    

    
    final_df['price'] = np.where(final_df['event']=='Refund', -1 * final_df['price'], final_df['price'])
    final_df['commissionRate'] = np.where(final_df['proceedsReason']=='Rate After One Year', 0.85, 0.70)
    final_df['proceeds'] = final_df['price'] * final_df['commissionRate'] * final_df['quantity']

    selected_date_conditions = (
        (final_df['appAppleId'] == app_id) &
        (final_df['originalStartDate'] >= start) & 
        (final_df['originalStartDate'] <= end) 
    )
    selected_df = final_df[selected_date_conditions]

    overall_date_conditions = (
    (final_df['appAppleId'] == app_id) &
    (final_df['originalStartDate'] <= '2024-04-01') #bu tarih ve oncesini tahminlemede kullanir
    )
    overall_df = final_df[overall_date_conditions]   

    final_dict_predicted = {
        "appAppleId":[],
        "subscriptionAppleId":[],
        "predictedLtv":[],
        "newSubs":[]
    }

    for package in overall_df['subscriptionAppleId'].unique():

        #OVERALL RETENTION USER QUANTITY
        without_refund_quantity_array = overall_df[(overall_df['subscriptionAppleId'] == package) & (overall_df['event'] != 'Refund')].groupby(by = 'consecutivePaidPeriods')['quantity'].sum().to_numpy()
        refund_quantity_array = overall_df[(overall_df['subscriptionAppleId'] == package) & (overall_df['event'] == 'Refund')].groupby(by = 'consecutivePaidPeriods')['quantity'].sum().to_numpy()
        if refund_quantity_array.shape[0] > without_refund_quantity_array.shape[0]:
            refund_quantity_array = refund_quantity_array[:without_refund_quantity_array.shape[0]]
        else:
            refund_quantity_array = np.pad(refund_quantity_array, (0, without_refund_quantity_array.shape[0] - refund_quantity_array.shape[0]))

        overall_retention_quantity_array = without_refund_quantity_array - refund_quantity_array

        #OVERALL RETENTION USER PROCEEDS
        without_refund_proceeds_array = overall_df[(overall_df['subscriptionAppleId'] == package) & (overall_df['event'] != 'Refund')].groupby(by = 'consecutivePaidPeriods')['proceeds'].sum().to_numpy()
        refund_proceeds_array = overall_df[(overall_df['subscriptionAppleId'] == package) & (overall_df['event'] == 'Refund')].groupby(by = 'consecutivePaidPeriods')['proceeds'].sum().to_numpy()
        if refund_proceeds_array.shape[0] > without_refund_proceeds_array.shape[0]:
            refund_proceeds_array = refund_proceeds_array[:without_refund_proceeds_array.shape[0]]
        else:
            refund_proceeds_array = np.pad(refund_proceeds_array, (0, without_refund_proceeds_array.shape[0] - refund_proceeds_array.shape[0]))

        overall_retention_proceeds_array = without_refund_proceeds_array + refund_proceeds_array #refund data is negative




        #SELECTED DATE RETENTION USER QUANTITY
        without_refund_quantity_array_selected = selected_df[(selected_df['subscriptionAppleId'] == package) & (selected_df['event'] != 'Refund')].groupby(by = 'consecutivePaidPeriods')['quantity'].sum().to_numpy()
        refund_quantity_array_selected = selected_df[(selected_df['subscriptionAppleId'] == package) & (selected_df['event'] == 'Refund')].groupby(by = 'consecutivePaidPeriods')['quantity'].sum().to_numpy()

        if refund_quantity_array_selected.size == 0 or refund_quantity_array_selected.shape[0] != without_refund_quantity_array_selected.shape[0]:
            refund_quantity_array_selected = np.zeros_like(without_refund_quantity_array_selected)

        selected_retention_quantity_array = without_refund_quantity_array_selected - refund_quantity_array_selected

        #SELECTED DATE RETENTION USER PROCEEDS
        without_refund_proceeds_array_selected = selected_df[(selected_df['subscriptionAppleId'] == package) & (selected_df['event'] != 'Refund')].groupby(by = 'consecutivePaidPeriods')['proceeds'].sum().to_numpy()
        refund_proceeds_array_selected = selected_df[(selected_df['subscriptionAppleId'] == package) & (selected_df['event'] == 'Refund')].groupby(by = 'consecutivePaidPeriods')['proceeds'].sum().to_numpy()

        if refund_proceeds_array_selected.size == 0 or refund_proceeds_array_selected.shape[0] != without_refund_proceeds_array_selected.shape[0]:
            refund_proceeds_array_selected = np.zeros_like(without_refund_proceeds_array_selected)

        selected_retention_proceeds_array = without_refund_proceeds_array_selected + refund_proceeds_array_selected #refund data is negative




        if overall_df[overall_df['subscriptionAppleId'] == package]['standardSubscriptionDuration'].iloc[0] == '7 Days':
            maxLtvRange = 57
            predictionPeriods = 5
        elif overall_df[overall_df['subscriptionAppleId'] == package]['standardSubscriptionDuration'].iloc[0] == '1 Month':
            maxLtvRange = 13
            predictionPeriods = 2
        elif overall_df[overall_df['subscriptionAppleId'] == package]['standardSubscriptionDuration'].iloc[0] == '1 Year':
            maxLtvRange = 2
            predictionPeriods = 1
            first_year_retention = 0.15 


        if (overall_df[overall_df['subscriptionAppleId'] == package]['standardSubscriptionDuration'].iloc[0] == '7 Days'):
            ignoredPeriods = -4
        elif (overall_df[overall_df['subscriptionAppleId'] == package]['standardSubscriptionDuration'].iloc[0] == '1 Month'):
            ignoredPeriods = -1
        else:
            ignoredPeriods = None

        originalRetentionArray = selected_retention_proceeds_array[0:predictionPeriods]
        originalRetentionArray = np.sort(selected_retention_proceeds_array)[::-1][:ignoredPeriods]  #son ignoredPeriods kadar period'u dahil etme.
        overallRetentionArray = np.copy(overall_retention_proceeds_array)
        #print('BEFORE PROCESS ORIGINAL', package, originalRetentionArray)

        if len(overallRetentionArray) < maxLtvRange and len(overallRetentionArray)>2:
            for i in range(len(overallRetentionArray), maxLtvRange):
                    newRetentionValue = (overallRetentionArray[-1]/overallRetentionArray[-2])*overallRetentionArray[-1]
                    overallRetentionArray = np.append(overallRetentionArray, newRetentionValue)

        try: 
            for i in range(len(originalRetentionArray), maxLtvRange):
                if originalRetentionArray[-1] == 0:
                    newRetentionValue = 0
                    originalRetentionArray = np.append(originalRetentionArray, newRetentionValue)
                else:
                    try:
                        newRetentionValue = int(originalRetentionArray[-1] + originalRetentionArray[-1] * ((overallRetentionArray[len(originalRetentionArray)] - overallRetentionArray[len(originalRetentionArray)-1]) / overallRetentionArray[len(originalRetentionArray)-1]))
                        originalRetentionArray = np.append(originalRetentionArray, newRetentionValue)
                    except IndexError:
                        pass
        except IndexError:
            pass

        #print("ORIGINAL", package, originalRetentionArray)

        try:    
            ltv = originalRetentionArray.sum()/selected_retention_quantity_array[0]
        except IndexError:
            ltv = 0
        final_dict_predicted["appAppleId"].append(app_id)
        final_dict_predicted["subscriptionAppleId"].append(package)
        final_dict_predicted["predictedLtv"].append(ltv)
        try:
            final_dict_predicted["newSubs"].append(selected_retention_quantity_array[0])
        except IndexError:
            final_dict_predicted["newSubs"].append(0)

    final_dict_predicted_df = pd.DataFrame(final_dict_predicted)  
    final_dict_predicted_df.dropna(inplace = True)      

    blendedLtv = round(sum(final_dict_predicted_df['predictedLtv']*final_dict_predicted_df['newSubs'])/final_dict_predicted_df['newSubs'].sum(), 2)
    success = st.success('One of the calculations has been completed. Please wait a moment.', icon="✅")
    time.sleep(0.25)
    success.empty()
    return blendedLtv




st.write("")
st.markdown("<h3 style='text-align: left; color: rgb(70, 50, 180);'>ROAS</h3>", unsafe_allow_html=True)

with st.form("roasView"):
    col1, col2 = st.columns(2)
    with col1:
        apps = df_packages['appName'].unique()
        app_option = st.selectbox('APP', apps)
        app_id = int(df_packages[df_packages['appName'] == '{}'.format(app_option)]['appAppleId'].unique()[0])
        start = datetime.strftime(st.date_input('SPEND FROM',  
                                                value = date.today() - timedelta(days=17), 
                                                help = 'Start date of the spending.', 
                                                format = 'YYYY-MM-DD',
                                                min_value = date(2024, 1, 1)), '%Y-%m-%d')
    with col2:
        countries = df_countries['countryname'].unique()
        country_option = st.selectbox('COUNTRY', countries)
        end = datetime.strftime(st.date_input('SPEND TO', 
                                                value = date.today() - timedelta(days=10), 
                                                help = 'End date of the spending.', 
                                                format = 'YYYY-MM-DD',
                                                max_value = date.today() - timedelta(days=10)), '%Y-%m-%d')
        
    period_dict = {'Weekly' : '7D', 'Monthly' : '1M'}
    period_option = st.selectbox('PERIOD', ('Weekly', 'Monthly'))

    submit_button_roasView = st.form_submit_button("SUBMIT")

    st.write("")
    st.write("")

    if submit_button_roasView:
        if country_option == 'GLOBAL':
            adjust_install_date_agg_spend_query = """
            SELECT "installDate", 
                    "appAppleId",  
                    SUM("spend") AS spend
            FROM adjust_install_date_agg
            WHERE "spend" > 0
            AND "installDate" >= '{}'
            AND "installDate" <= '{}'
            AND "appAppleId" = '{}'
            GROUP BY "installDate", "appAppleId"
            """.format(start, end, app_id)

            spends_df = read_sql_query_io(adjust_install_date_agg_spend_query)

            app_store_connect_events_query = """        
            SELECT "originalStartDate",
                "eventDate",
                "event",
                "appAppleId",
                "subscriptionAppleId",
                "proceedsReason",
                "consecutivePaidPeriods",
                SUM("quantity") as "quantity"
            FROM subscription_event
            WHERE "event" IN ('Subscribe',
                            'Paid Subscription from Introductory Offer',
                            'Reactivate',
                            'Refund',
                            'Renew',
                            'Renewal from Billing Retry',
                            'Renewals from Grace Period',
                            'Reactivate with Crossgrade',
                            'Reactivate with Downgrade',
                            'Reactivate with Upgrade')
            AND "eventDate" >= '{}'
            AND "appAppleId" = '{}'
            GROUP BY "originalStartDate", "eventDate", "event", "appAppleId", "subscriptionAppleId", "proceedsReason", "consecutivePaidPeriods"
            """.format(start, app_id)

            events_df = read_sql_query_io(app_store_connect_events_query)
            print("GLOBAL DONE APP STORE CONNECT EVENTS DF")

        else:
            adjust_install_date_agg_spend_query = """
            SELECT "installDate", 
                    "appAppleId", 
                    "countryCode", 
                    SUM("spend") AS spend
            FROM adjust_install_date_agg
            WHERE "spend" > 0
            AND "installDate" >= '{}'
            AND "installDate" <= '{}'
            AND "appAppleId" = '{}'
            AND "countryCode" = '{}'
            GROUP BY "installDate", "appAppleId", "countryCode"
            """.format(start, end, app_id, df_countries[df_countries['countryname'] == '{}'.format(country_option)]['countrycode'].unique()[0])

            spends_df = read_sql_query_io(adjust_install_date_agg_spend_query)

            app_store_connect_events_query = """        
            SELECT "originalStartDate",
                "eventDate",
                "event",
                "appAppleId",
                "country",
                "subscriptionAppleId",
                "proceedsReason",
                "consecutivePaidPeriods",
                SUM("quantity") as "quantity"
            FROM subscription_event
            WHERE "event" IN ('Subscribe',
                            'Paid Subscription from Introductory Offer',
                            'Reactivate',
                            'Refund',
                            'Renew',
                            'Renewal from Billing Retry',
                            'Renewals from Grace Period',
                            'Reactivate with Crossgrade',
                            'Reactivate with Downgrade',
                            'Reactivate with Upgrade')
            AND "eventDate" >= '{}'
            AND "appAppleId" = '{}'
            AND "country" = '{}'
            GROUP BY "originalStartDate", "eventDate", "event", "appAppleId", "country", "subscriptionAppleId", "proceedsReason", "consecutivePaidPeriods"
            """.format(start, app_id, df_countries[df_countries['countryname'] == '{}'.format(country_option)]['countrycode'].unique()[0])

            events_df = read_sql_query_io(app_store_connect_events_query)
            print("COUNTRY DONE APP STORE CONNECT EVENTS DF")

        final_df = events_df.merge(package_details_df, 
                           how='left',
                           on = ['appAppleId', 'subscriptionAppleId'])
        
        final_df['price'] = np.where(final_df['event']=='Refund', -1 * final_df['price'], final_df['price'])
        final_df['commissionRate'] = np.where(final_df['proceedsReason']=='Rate After One Year', 0.85, 0.70)
        final_df['proceeds'] = final_df['price'] * final_df['commissionRate'] * final_df['quantity']
        final_df["originalStartDate"] = pd.to_datetime(final_df["originalStartDate"])

        grouped_data_proceeds_final = pd.DataFrame()

        date_object = datetime.strptime(start, '%Y-%m-%d')
        difference = (datetime.today() - date_object + timedelta(days=7)).days


        roas_days = [10, 30, 60, 90, 180, 270, 365]
        roas_days_modified = [value for value in roas_days if value < difference]

        for day in roas_days_modified:
            filtered_data = final_df[
                (final_df["originalStartDate"] >= '{}'.format(start)) &
                (final_df["originalStartDate"] <= '{}'.format(end)) &
                (final_df["eventDate"] <= (final_df["originalStartDate"] +  timedelta(days=day)))
                                    ]

            grouped_data_proceeds = filtered_data.groupby(pd.Grouper(key="originalStartDate", 
                                                                        freq="{}".format(period_dict[period_option]),
                                                                        origin='{}'.format(pd.to_datetime(start)))).agg({"proceeds": np.sum})
            
            grouped_data_proceeds.reset_index(inplace=True)
            grouped_data_proceeds.sort_values(by='originalStartDate', ascending=True, inplace=True)
            grouped_data_proceeds.rename(columns = {'proceeds': 'proceeds_{}'.format(day)}, inplace = True)
            grouped_data_proceeds.rename(columns = {'originalStartDate': 'originalStartDate_{}'.format(day)}, inplace = True)

            grouped_data_proceeds_final = pd.concat([grouped_data_proceeds_final, grouped_data_proceeds], axis=1)
        
        grouped_data_proceeds_final = grouped_data_proceeds_final.filter(regex='^originalStartDate_10|^proceeds')
        grouped_data_proceeds_final.rename(columns = {'originalStartDate_10': 'originalStartDate'}, inplace = True)

        spends_df['installDate'] = pd.to_datetime(spends_df['installDate'])
        grouped_data_spend = spends_df.groupby(pd.Grouper(key="installDate", 
                                                                    freq="{}".format(period_dict[period_option]),
                                                                    origin='{}'.format(pd.to_datetime(start)))).agg({"spend": np.sum})

        main_df = grouped_data_spend.merge(grouped_data_proceeds_final, how='left', left_on='installDate', right_on='originalStartDate')
        column_to_move = main_df.pop('spend')
        main_df.insert(1, 'spend', column_to_move)

        main_df["originalStartDate"] = pd.to_datetime(main_df["originalStartDate"]).dt.strftime('%Y-%m-%d')

        for columns_to_divided in main_df.filter(regex='^proceeds'):
            main_df['roas_' + columns_to_divided] = main_df[columns_to_divided] / main_df['spend']

        if 'proceeds_10' not in main_df:
            main_df['proceeds_10'] = 0
        if 'proceeds_30' not in main_df:
            main_df['proceeds_30'] = 0
        if 'proceeds_60' not in main_df:
            main_df['proceeds_60'] = 0
        if 'proceeds_90' not in main_df:
            main_df['proceeds_90'] = 0
        if 'proceeds_180' not in main_df:
            main_df['proceeds_180'] = 0
        if 'proceeds_270' not in main_df:
            main_df['proceeds_270'] = 0
        if 'proceeds_365' not in main_df:
            main_df['proceeds_365'] = 0

        if 'roas_proceeds_10' not in main_df:
            main_df['roas_proceeds_10'] = 0
        if 'roas_proceeds_30' not in main_df:
            main_df['roas_proceeds_30'] = 0
        if 'roas_proceeds_60' not in main_df:
            main_df['roas_proceeds_60'] = 0
        if 'roas_proceeds_90' not in main_df:
            main_df['roas_proceeds_90'] = 0
        if 'roas_proceeds_180' not in main_df:
            main_df['roas_proceeds_180'] = 0
        if 'roas_proceeds_270' not in main_df:
            main_df['roas_proceeds_270'] = 0
        if 'roas_proceeds_365' not in main_df:
            main_df['roas_proceeds_365'] = 0


        main_df_table = main_df[['originalStartDate',
                        'spend',
                        'roas_proceeds_10',
                        'roas_proceeds_30',
                        'roas_proceeds_60',
                        'roas_proceeds_90',
                        'roas_proceeds_180',
                        'roas_proceeds_270',
                        'roas_proceeds_365']]
        
        AG_GRID_PERCENT_FORMATTER = JsCode(
            """
            function customPercentFormatter(params) {
                let n = Number.parseFloat(params.value) * 100;
                let precision = params.column.colDef.precision ?? 0;

                if (!Number.isNaN(n)) {
                return n.toFixed(precision).replace(/\B(?=(\d{3})+(?!\d))/g, ',')+'%';
                } else {
                return '-';
                }
            }
            """
        )

        custom_css = {
        ".ag-row-hover": {"background-color": "rgba(204, 143, 37, 0.7) !important"}
        }
        
        gb = GridOptionsBuilder.from_dataframe(main_df_table)
        gb.configure_column("originalStartDate", headerName = "Date", type=["date"])
        gb.configure_column("spend", headerName = "Spend", type=["numericColumn","numberColumnFilter","customNumericFormat"],  valueGetter="data.spend.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits:2})")
        gb.configure_column("proceeds_365", headerName = "365D Proceeds", type=["numericColumn","numberColumnFilter","customNumericFormat"],  valueGetter="data.proceeds_365.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits:2})")
        gb.configure_column("roas_proceeds_10", header_name="ROAS 10D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_30", header_name="ROAS 30D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_60", header_name="ROAS 60D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_90", header_name="ROAS 90D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_180", header_name="ROAS 180D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_270", header_name="ROAS 270D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)
        gb.configure_column("roas_proceeds_365", header_name="ROAS 365D", type=["numberColumnFilter", "customNumericFormat", "numericColumn"], valueFormatter=AG_GRID_PERCENT_FORMATTER, precision=2)

        vgo = gb.build()
        AgGrid(main_df_table, gridOptions=vgo, fit_columns_on_grid_load = True,  allow_unsafe_jscode=True, custom_css= custom_css)

        st.write("")
        st.write("")
        st.write("")

        roas_df = pd.DataFrame()

        roas_df['Days'] = [
            10,
            30,
            60,
            90,
            180,
            270,
            365
        ]

        roas_df['ROAS'] = [
            main_df['proceeds_10'].sum()/main_df['spend'].sum(),
            main_df['proceeds_30'].sum()/main_df['spend'].sum(),
            main_df['proceeds_60'].sum()/main_df['spend'].sum(),
            main_df['proceeds_90'].sum()/main_df['spend'].sum(),
            main_df['proceeds_180'].sum()/main_df['spend'].sum(),
            main_df['proceeds_270'].sum()/main_df['spend'].sum(),
            main_df['proceeds_365'].sum()/main_df['spend'].sum()
        ]

        st.divider()
        st.write("")
        st.write("")
        col1, col2 , col3= st.columns(3)

        with col1:
            st.metric(label=":red[Total Spend]", value="${}".format(prettify(round(spends_df['spend'].sum(), 2))))

        with col2:
            st.metric(label=":green[Total Proceeds]", value="${}".format(prettify(round(final_df[(final_df["originalStartDate"] >= '{}'.format(start)) & (final_df["originalStartDate"] <= '{}'.format(end))]['proceeds'].sum(), 2))))

        with col3:
            st.metric(label=":violet[ROAS]", value="%{}".format(prettify(round(100*final_df[(final_df["originalStartDate"] >= '{}'.format(start)) & (final_df["originalStartDate"] <= '{}'.format(end))]['proceeds'].sum()/spends_df['spend'].sum(), 2))))

        st.write("")
        st.write("")
        st.write("")
        
        line_chart = alt.Chart(
            roas_df[roas_df['ROAS']> 0], 
        ).mark_line(
            point = True
        ).encode(
            y = alt.Y('ROAS', title = 'ROAS', axis=alt.Axis(format=".2%")), 
            x = alt.X('Days:N', title = 'DAYS', scale=alt.Scale(domain=roas_df['Days'].values), axis=alt.Axis(values=roas_df['Days'].values))
        )

        breakeven_line = alt.Chart(pd.DataFrame({'y': [1]})).mark_rule(color='orange').encode(y=alt.Y('y', axis=alt.Axis(format=".2%")))

        final_roas_chart = alt.layer(breakeven_line, line_chart).configure_mark(
                                                opacity = 0.95, 
                                                color = 'rgb(70, 50, 180)'
                                            ).configure_axis(
                                                titleColor = 'rgb(70, 50, 180)', 
                                                titleFontWeight = 'bold', 
                                                titleFontSize = 17, 
                                                gridOpacity = 0.7,
                                                labelAngle = 0
                                            )
        
        st.altair_chart(final_roas_chart, use_container_width = True)

    st.toast('Ready to use! Bon appetit.', icon='🥪')


st.write("")
st.write("")
st.write("")
st.write("")
st.markdown("<h3 style='text-align: left; color: rgb(153, 255, 204);'>Blended Predicted LTV</h3>", unsafe_allow_html=True)

with st.form("LTV_View"):
    #col1, col2 = st.columns(2)
    #st.error('Minimum selectable date for :violet["Math AI"] is :violet["May 2023"]. If you select an earlier date, you get an error.', icon="⚠️")

    #with col1:
    apps = df_packages['appName'].unique()
    app_option = st.selectbox('APP', apps)
    app_id = int(df_packages[df_packages['appName'] == '{}'.format(app_option)]['appAppleId'].unique()[0])
        #start = datetime.strftime(st.date_input('FROM',  
        #                                        value = date.today() - timedelta(days=60), 
        #                                        help = 'Original start date of the subscription. This is a cohort type data.', 
        #                                        format = 'YYYY-MM-DD',
        #                                        min_value = date(2022, 1, 1)), '%Y-%m-%d')
        
        #kpi_option = st.selectbox('KPI', ('Predicted LTV', 'Realized LTV', 'Blended CAC', 'PLTV/CAC'))

    #with col2:
        #countries = df_countries['countryname'].unique()
        #country_option = st.selectbox('COUNTRY', countries)
        #end = datetime.strftime(st.date_input('TO', 
        #                                        value = date.today() - timedelta(days=30), 
        #                                        format = 'YYYY-MM-DD',
        #                                        max_value = date.today() - timedelta(days=30)), '%Y-%m-%d')

        #period_option = st.selectbox('PERIOD', ('Weekly', 'Monthly'))
    
    # Ülke seçimi
    countries = df_countries['countryname'].unique()
    country_option = st.selectbox('COUNTRY', countries)

    st.write("")
    submit_button_detailedAppView = st.form_submit_button("SUBMIT")
    st.write("")

    if submit_button_detailedAppView:
        start_date = datetime(2023, 6, 1)
        end_date = datetime.today().replace(day=1) - timedelta(days=1)
        data = []
        current_date = start_date
        valid_data_found = False  # Geçerli veri olup olmadığını izlemek için bir flag

        while current_date <= end_date:
            start = current_date.replace(day=1)
            last_day = calendar.monthrange(current_date.year, current_date.month)[1]
            end = current_date.replace(day=last_day)

            start = datetime.strftime(start, '%Y-%m-%d')
            end = datetime.strftime(end, '%Y-%m-%d')

            # Seçilen ülkeyi predicted_ltv fonksiyonuna geçir
            ltv = predicted_ltv(country_option)  # Ülke seçeneğini geç

            if ltv is not None and not pd.isna(ltv):  # LTV değeri geçerliyse (None veya NaN değilse)
                valid_data_found = True
                month_str = current_date.strftime('%Y-%m')
                data.append({'Month': month_str, 'LTV': ltv})
            
            next_month = current_date.replace(day=28) + timedelta(days=4)  # Her zaman bir sonraki aya geçer
            current_date = next_month.replace(day=1)

        # Eğer geçerli veri bulunmamışsa No values found mesajı göster
        if not valid_data_found:
            st.toast('No values found!', icon='⚠️')
            st.warning('No values found!', icon="⚠️")
        else:
            df = pd.DataFrame(data)
            #print("MANUEL", predicted_ltv())
            df['Month'] = pd.to_datetime(df['Month'])

            ltv_chart = alt.Chart(df).mark_line(point=True, color='rgb(70, 50, 180)').encode(
                x=alt.X('yearmonth(Month):T', title='Month', axis=alt.Axis(format='%Y-%m', labelAngle=-45)),
                y=alt.Y('LTV:Q', title='LTV'),
            ).configure_mark(
                opacity=0.95,
                color='rgb(153, 255, 204)'
            ).configure_axis(
                titleColor='rgb(204, 143, 37)',
                titleFontWeight='bold',
                titleFontSize=17,
                gridOpacity=0.7
            )

            st.write("")
            st.write("")
            st.altair_chart(ltv_chart, use_container_width=True)
            st.write("")
            st.write("")
            st.toast('Ready to use! Bon appetit.', icon='🥪')
